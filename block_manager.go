package lstore

import (
	"github.com/hashicorp/golang-lru"
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

type blockManagerConfig struct {
	indexingStrategyConfig
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
	indexingStrategy *indexingStrategy
	blockCache       *lru.ARCCache
	blockHashCache   *lru.ARCCache
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
	blockHashCache, _ := lru.NewARC(64)
	indexingStrategy := newIndexingStrategy(&config.indexingStrategyConfig)
	return &blockManager{
		indexingStrategy: indexingStrategy,
		blockCache:       blockCache,
		blockHashCache:   blockHashCache,
		dataManager:      newDataManager(config.BlockDirectory, config.BlockFileSizeInPowerOfTwo),
	}
}

func (mgr *blockManager) Close() error {
	return mgr.dataManager.Close()
}

func (mgr *blockManager) writeBlock(seq blockSeq, block *block) (blockSeq, blockHash, error) {
	blkHash := block.Hash(mgr.indexingStrategy)
	mgr.blockHashCache.Add(seq, blkHash)
	mgr.blockCache.Add(seq, block)
	stream := gocodec.NewStream(nil)
	blockOriginalSize, blockCompressedSize, compressedBlock :=
		mgr.compressBlock(stream, block)
	stream.Reset(nil)
	stream.Marshal(compressedBlockHeader{
		blockOriginalSize:   blockOriginalSize,
		blockCompressedSize: blockCompressedSize,
	})
	if stream.Error != nil {
		return 0, nil, stream.Error
	}
	header := stream.Buffer()
	err := mgr.dataManager.writeBuf(uint64(seq), header)
	if err != nil {
		return 0, nil, err
	}
	tailBlockSeq := seq + blockSeq(len(header))
	err = mgr.dataManager.writeBuf(uint64(tailBlockSeq), compressedBlock)
	if err != nil {
		return 0, nil, err
	}
	tailBlockSeq += blockSeq(len(compressedBlock))
	return tailBlockSeq, blkHash, nil
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

func (mgr *blockManager) readBlockHash(blockSeq blockSeq) (blockHash, error) {
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
		return nil, iter.Error
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
