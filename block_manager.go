package lstore

import (
	"github.com/hashicorp/golang-lru"
	"os"
	"sync"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/lstore/ref"
	"path"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/esdb/lstore/lz4"
)

type compressedBlockHeader struct {
	blockOriginalSize   uint32
	blockCompressedSize uint32
}

var compressedBlockHeaderSize = calcCompressedBlockHeaderSize()

func calcCompressedBlockHeaderSize() uint32 {
	stream := gocodec.NewStream(nil)
	return uint32(stream.Marshal(compressedBlockHeader{1, 1}))
}

type BlockManagerConfig struct {
	IndexingStrategyConfig
	BlockDirectory            string
	BlockFileSizeInPowerOfTwo uint8
	BlockCacheSize            int
}

// blockManager is global per store
// it manages the read/write to block file
// compress/decompress block from the mmap
// retain lru block cache
type blockManager struct {
	indexingStrategy          *IndexingStrategy
	blockDirectory            string
	blockFileSizeInPowerOfTwo uint8 // 2 ^ x
	blockFileSize             uint64
	blockCache                *lru.ARCCache
	blockHashCache            *lru.ARCCache
	// tmp assume there is single writer
	blockCompressTmp []byte
	// only lock the modification of following maps
	// does not cover reading or writing
	mapMutex   *sync.Mutex
	files      map[BlockSeq]*os.File
	writeMMaps map[BlockSeq]mmap.MMap
	readMMaps  map[BlockSeq]mmap.MMap
}

func newBlockManager(config *BlockManagerConfig) *blockManager {
	if config.BlockFileSizeInPowerOfTwo == 0 {
		config.BlockFileSizeInPowerOfTwo = 30
	}
	if config.BlockCacheSize == 0 {
		config.BlockCacheSize = 1024
	}
	blockCache, _ := lru.NewARC(config.BlockCacheSize)
	blockHashCache, _ := lru.NewARC(64)
	indexingStrategy := NewIndexingStrategy(&config.IndexingStrategyConfig)
	return &blockManager{
		indexingStrategy:          indexingStrategy,
		blockDirectory:            config.BlockDirectory,
		blockFileSizeInPowerOfTwo: config.BlockFileSizeInPowerOfTwo,
		blockFileSize:             2 << config.BlockFileSizeInPowerOfTwo,
		blockCache:                blockCache,
		blockHashCache:            blockHashCache,
		mapMutex:                  &sync.Mutex{},
		files:                     map[BlockSeq]*os.File{},
		readMMaps:                 map[BlockSeq]mmap.MMap{},
		writeMMaps:                map[BlockSeq]mmap.MMap{},
	}
}

func (mgr *blockManager) Close() error {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	var errs []error
	for _, writeMMap := range mgr.writeMMaps {
		err := writeMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!block_manager.failed to close writeMMap", "err", err)
		}
	}
	for _, readMMap := range mgr.readMMaps {
		err := readMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!block_manager.failed to close readMMap", "err", err)
		}
	}
	for _, file := range mgr.files {
		err := file.Close()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!block_manager.failed to close file", "err", err)
		}
	}
	return ref.NewMultiError(errs)
}

func (mgr *blockManager) writeBlock(blockSeq BlockSeq, block *block) (BlockSeq, error) {
	mgr.blockHashCache.Add(blockSeq, block.Hash(mgr.indexingStrategy))
	mgr.blockCache.Add(blockSeq, block)
	stream := gocodec.NewStream(nil)
	blockOriginalSize, blockCompressedSize, compressedBlock :=
		mgr.compressBlock(stream, block)
	stream.Reset(nil)
	stream.Marshal(compressedBlockHeader{
		blockOriginalSize:   blockOriginalSize,
		blockCompressedSize: blockCompressedSize,
	})
	if stream.Error != nil {
		return 0, stream.Error
	}
	header := stream.Buffer()
	err := mgr.writeBuf(blockSeq, header)
	if err != nil {
		return 0, err
	}
	tailBlockSeq := blockSeq + BlockSeq(len(header))
	err = mgr.writeBuf(tailBlockSeq, compressedBlock)
	if err != nil {
		return 0, err
	}
	tailBlockSeq += BlockSeq(len(compressedBlock))
	return tailBlockSeq, nil
}

func (mgr *blockManager) compressBlock(stream *gocodec.Stream, block *block) (uint32, uint32, []byte) {
	stream.Reset(nil)
	stream.Marshal(*block)
	if stream.Error != nil {
		panic(stream.Error)
	}
	buf := stream.Buffer()
	compressBound := lz4.CompressBound(len(buf))
	if compressBound > len(mgr.blockCompressTmp) {
		mgr.blockCompressTmp = make([]byte, compressBound)
	}
	compressedSize := lz4.CompressDefault(buf, mgr.blockCompressTmp)
	return uint32(len(buf)), uint32(compressedSize), mgr.blockCompressTmp[:compressedSize]
}

func (mgr *blockManager) writeBuf(blockSeq BlockSeq, buf []byte) error {
	fileBlockSeq := blockSeq >> mgr.blockFileSizeInPowerOfTwo
	relativeOffset := int(blockSeq - (fileBlockSeq << mgr.blockFileSizeInPowerOfTwo))
	writeMMap, err := mgr.openWriteMMap(fileBlockSeq)
	if err != nil {
		return err
	}
	dst := writeMMap[relativeOffset:]
	copiedBytesCount := copy(dst, buf)
	buf = buf[copiedBytesCount:]
	if len(buf) > 0 {
		return mgr.writeBuf(blockSeq+BlockSeq(copiedBytesCount), buf)
	}
	return nil
}

func (mgr *blockManager) readBlockHash(blockSeq BlockSeq) (blockHash, error) {
	blkHashCache, found := mgr.blockHashCache.Get(blockSeq)
	if found {
		return blkHashCache.(blockHash), nil
	}
	blk, err := mgr.readBlock(blockSeq)
	if err != nil {
		return nil, err
	}
	return blk.Hash(mgr.indexingStrategy), nil
}

func (mgr *blockManager) readBlock(blockSeq BlockSeq) (*block, error) {
	blkObj, found := mgr.blockCache.Get(blockSeq)
	if found {
		return blkObj.(*block), nil
	}
	headerBuf, err := mgr.readBuf(blockSeq, compressedBlockHeaderSize)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(headerBuf)
	compressedBlockHeader, _ := iter.Unmarshal((*compressedBlockHeader)(nil)).(*compressedBlockHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	decompressed := make([]byte, compressedBlockHeader.blockOriginalSize)
	compressedBuf, err := mgr.readBuf(
		blockSeq+BlockSeq(compressedBlockHeaderSize),
		compressedBlockHeader.blockCompressedSize)
	if err != nil {
		return nil, err
	}
	lz4.DecompressSafe(compressedBuf, decompressed)
	iter.Reset(decompressed)
	blk, _ := iter.Unmarshal((*block)(nil)).(*block)
	if iter.Error != nil {
		return nil, iter.Error
	}
	mgr.blockCache.Add(blockSeq, blk)
	return blk, nil
}

func (mgr *blockManager) readBuf(blockSeq BlockSeq, remainingSize uint32) ([]byte, error) {
	fileBlockSeq := blockSeq >> mgr.blockFileSizeInPowerOfTwo
	relativeOffset := blockSeq - (fileBlockSeq << mgr.blockFileSizeInPowerOfTwo)
	readMMap, err := mgr.openReadMMap(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	buf := readMMap[relativeOffset:]
	if uint32(len(buf)) < remainingSize {
		remainingSize -= uint32(len(buf))
		moreBuf, err := mgr.readBuf(blockSeq+BlockSeq(len(buf)), remainingSize)
		if err != nil {
			return nil, err
		}
		return append(buf, moreBuf...), nil
	}
	return buf[:remainingSize], nil
}

func (mgr *blockManager) openReadMMap(fileBlockSeq BlockSeq) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		return nil, err
	}
	mgr.readMMaps[fileBlockSeq] = readMMap
	return readMMap, nil
}

func (mgr *blockManager) openWriteMMap(fileBlockSeq BlockSeq) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("map RDWR for block failed: %s", err.Error())
	}
	mgr.writeMMaps[fileBlockSeq] = writeMMap
	return writeMMap, nil
}

func (mgr *blockManager) openFile(fileBlockSeq BlockSeq) (*os.File, error) {
	file := mgr.files[fileBlockSeq]
	if file != nil {
		return file, nil
	}
	filePath := path.Join(mgr.blockDirectory, fmt.Sprintf(
		"%d.block", fileBlockSeq<<mgr.blockFileSizeInPowerOfTwo))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		os.MkdirAll(path.Dir(filePath), 0777)
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		err = file.Truncate(int64(mgr.blockFileSize))
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	mgr.files[fileBlockSeq] = file
	return file, nil
}
