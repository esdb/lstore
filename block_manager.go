package lstore

import (
	"github.com/esdb/gocodec"
	"github.com/esdb/lstore/lz4"
	"fmt"
	"unsafe"
	"github.com/esdb/lstore/dheap"
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
	BlockDirectory            string
	BlockFileSizeInPowerOfTwo uint8
	BlockCompressed           bool
}

type blockManager interface {
	writeBlock(seq blockSeq, block *block) (blockSeq, blockSeq, error)
	readBlock(seq blockSeq) (*block, error)
}

// blockManager is global per store
// it manages the read/write to block file
// compress/decompress block from the mmap
// retain lru block cache
type mmapBlockManager struct {
	dataManager *dheap.DataManager
	// tmp assume there is single writer
	blockCompressTmp []byte
	blockCompressed  bool
}

func newBlockManager(config *blockManagerConfig) *mmapBlockManager {
	if config.BlockFileSizeInPowerOfTwo == 0 {
		config.BlockFileSizeInPowerOfTwo = 30
	}
	return &mmapBlockManager{
		blockCompressed: config.BlockCompressed,
		dataManager:     dheap.New(config.BlockDirectory, config.BlockFileSizeInPowerOfTwo),
	}
}

func (mgr *mmapBlockManager) Close() error {
	return mgr.dataManager.Close()
}

func (mgr *mmapBlockManager) writeBlock(seq blockSeq, block *block) (blockSeq, blockSeq, error) {
	var buf []byte
	if mgr.blockCompressed {
		buf = mgr.compressBlock(block)
	} else {
		buf = mgr.marshalBlock(block)
	}
	newSeq, err := mgr.dataManager.WriteBuf(uint64(seq), buf)
	if err != nil {
		return 0, 0, err
	}
	return blockSeq(newSeq), blockSeq(newSeq) + blockSeq(len(buf)), nil
}

func (mgr *mmapBlockManager) compressBlock(block *block) []byte {
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

func (mgr *mmapBlockManager) marshalBlock(block *block) []byte {
	stream := gocodec.NewStream(nil)
	stream.Marshal(*block)
	if stream.Error != nil {
		panic(stream.Error)
	}
	return stream.Buffer()
}

func (mgr *mmapBlockManager) readBlock(seq blockSeq) (blk *block, err error) {
	if mgr.blockCompressed {
		blk, err = mgr.uncompressBlock(seq)
	} else {
		blk, err = mgr.unmarshalBlock(seq)
	}
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (mgr *mmapBlockManager) uncompressBlock(seq blockSeq) (*block, error) {
	headerBuf, err := mgr.dataManager.ReadBuf(uint64(seq), compressedBlockHeaderSize)
	if err != nil {
		return nil, err
	}
	iter := gocodec.ReadonlyConfig.NewIterator(headerBuf)
	compressedBlockHeader, _ := iter.Unmarshal((*compressedBlockHeader)(nil)).(*compressedBlockHeader)
	if iter.Error != nil {
		return nil, fmt.Errorf("unmarshal compressedBlockHeader failed: %s", iter.Error)
	}
	decompressed := make([]byte, compressedBlockHeader.blockOriginalSize)
	compressedBuf, err := mgr.dataManager.ReadBuf(
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
	return blk, nil
}

func (mgr *mmapBlockManager) unmarshalBlock(seq blockSeq) (*block, error) {
	sizeBuf, err := mgr.dataManager.ReadBuf(uint64(seq), 8)
	if err != nil {
		return nil, err
	}
	size := (*uint64)(unsafe.Pointer(&sizeBuf[0]))
	buf, err := mgr.dataManager.ReadBuf(uint64(seq), uint32(*size))
	if err != nil {
		return nil, err
	}
	iter := gocodec.ReadonlyConfig.NewIterator(buf)
	blk, _ := iter.Unmarshal((*block)(nil)).(*block)
	if iter.Error != nil {
		return nil, fmt.Errorf("unmarshal block failed: %s", iter.Error)
	}
	return blk, nil
}

func (mgr *mmapBlockManager) remove(untilSeq blockSeq) {
	mgr.dataManager.Remove(uint64(untilSeq))
}
