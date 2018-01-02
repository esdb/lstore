package lstore

import (
	"github.com/hashicorp/golang-lru"
	"github.com/esdb/gocodec"
	"github.com/esdb/lstore/lz4"
	"fmt"
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

type blockManagerConfig struct {
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
	dataManager      *dataManager
	indexingStrategy *IndexingStrategy
	blockCache       *lru.ARCCache
	// tmp assume there is single writer
	blockCompressTmp []byte
}

func newBlockManager(config *blockManagerConfig) *blockManager {
	if config.BlockFileSizeInPowerOfTwo == 0 {
		config.BlockFileSizeInPowerOfTwo = 30
	}
	if config.BlockCacheSize == 0 {
		config.BlockCacheSize = 1024
	}
	blockCache, _ := lru.NewARC(config.BlockCacheSize)
	indexingStrategy := NewIndexingStrategy(&config.IndexingStrategyConfig)
	return &blockManager{
		indexingStrategy: indexingStrategy,
		blockCache:       blockCache,
		dataManager:      newDataManager(config.BlockDirectory, config.BlockFileSizeInPowerOfTwo),
	}
}

func (mgr *blockManager) Close() error {
	return mgr.dataManager.Close()
}

func (mgr *blockManager) writeBlock(seq blockSeq, block *block) (blockSeq, blockSeq, error) {
	mgr.blockCache.Add(seq, block)
	compressedBlock := mgr.compressBlock(block)
	newSeq, err := mgr.dataManager.writeBuf(uint64(seq), compressedBlock)
	if err != nil {
		return 0, 0, err
	}
	return blockSeq(newSeq), blockSeq(newSeq) + blockSeq(len(compressedBlock)), nil
}

func (mgr *blockManager) compressBlock(block *block) []byte {
	stream := gocodec.NewStream(nil)
	stream.Marshal(*block)
	if stream.Error != nil {
		panic(stream.Error)
	}
	buf := stream.Buffer()
	compressBound := lz4.CompressBound(len(buf)) + int(compressedBlockHeaderSize)
	if compressBound > len(mgr.blockCompressTmp) {
		mgr.blockCompressTmp = make([]byte, compressBound)
	}
	compressedSize := lz4.CompressDefault(buf, mgr.blockCompressTmp[compressedBlockHeaderSize:])
	stream.Reset(mgr.blockCompressTmp[:0])
	stream.Marshal(compressedBlockHeader{
		blockOriginalSize:   uint32(len(buf)),
		blockCompressedSize: uint32(compressedSize),
	})
	if stream.Error != nil {
		panic(stream.Error)
	}
	return mgr.blockCompressTmp[:compressedSize+int(compressedBlockHeaderSize)]
}

func (mgr *blockManager) readBlock(seq blockSeq) (*block, error) {
	blkObj, found := mgr.blockCache.Get(seq)
	if found {
		return blkObj.(*block), nil
	}
	headerBuf, err := mgr.dataManager.readBuf(uint64(seq), compressedBlockHeaderSize)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(headerBuf)
	compressedBlockHeader, _ := iter.Unmarshal((*compressedBlockHeader)(nil)).(*compressedBlockHeader)
	if iter.Error != nil {
		return nil, fmt.Errorf("unmarshal compressedBlockHeader failed: %s", iter.Error)
	}
	decompressed := make([]byte, compressedBlockHeader.blockOriginalSize)
	compressedBuf, err := mgr.dataManager.readBuf(
		uint64(seq)+uint64(compressedBlockHeaderSize),
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
	mgr.blockCache.Add(seq, blk)
	return blk, nil
}
