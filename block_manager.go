package lstore

import (
	"github.com/esdb/gocodec"
	"github.com/esdb/lstore/lz4"
	"fmt"
	"unsafe"
	"github.com/esdb/lstore/dheap"
	"github.com/esdb/lstore/mheap"
	"io"
	"github.com/v2pro/plz"
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
	io.Closer
	newReader(pageSizeInPowerOfTwo uint8, maxPagesCount int) blockReader
	newWriter() blockWriter
	lock(reader interface{}, lockedFrom Offset)
	unlock(reader interface{})
}

type blockReader interface {
	io.Closer
	readBlock(seq blockSeq) (*block, error)
	// gc might invalidate any block returned from previous readBlock
	gc()
}

type blockWriter interface {
	io.Closer
	writeBlock(seq blockSeq, block *block) (blockSeq, blockSeq, error)
	remove(untilSeq blockSeq)
}

// blockManager is global per store
// it manages the read/write to block file
// compress/decompress block from the mmap
// retain lru block cache
type mmapBlockManager struct {
	diskManager     *dheap.DiskManager
	blockCompressed bool
}

type mmapBlockReader struct {
	mmapBlockManager
	memoryManager *mheap.MemoryManager
	iter          *gocodec.Iterator
	cache         map[blockSeq]*block
}

type mmapBlockWriter struct {
	mmapBlockManager
	// tmp assume there is single writer
	blockCompressTmp []byte
	stream           *gocodec.Stream
}

func newBlockManager(config *blockManagerConfig) *mmapBlockManager {
	if config.BlockFileSizeInPowerOfTwo == 0 {
		config.BlockFileSizeInPowerOfTwo = 30
	}
	return &mmapBlockManager{
		blockCompressed: config.BlockCompressed,
		diskManager:     dheap.New(config.BlockDirectory, config.BlockFileSizeInPowerOfTwo),
	}
}

func (mgr *mmapBlockManager) Close() error {
	return plz.Close(mgr.diskManager)
}

func (mgr *mmapBlockManager) newReader(pageSizeInPowerOfTwo uint8, maxPagesCount int) blockReader {
	memMgr := mheap.New(pageSizeInPowerOfTwo, maxPagesCount)
	iter := gocodec.ReadonlyConfig.NewIterator(nil)
	iter.Allocator(memMgr)
	return &mmapBlockReader{
		mmapBlockManager: *mgr,
		memoryManager:    memMgr,
		iter:             iter,
		cache:            map[blockSeq]*block{},
	}
}

func (mgr *mmapBlockManager) newWriter() blockWriter {
	return &mmapBlockWriter{
		mmapBlockManager: *mgr,
		stream:           gocodec.NewStream(nil),
	}
}

func (mgr *mmapBlockManager) lock(reader interface{}, lockedFrom Offset) {
	mgr.diskManager.Lock(reader)
}

func (mgr *mmapBlockManager) unlock(reader interface{}) {
	mgr.diskManager.Unlock(reader)
}

func (writer *mmapBlockWriter) Close() error {
	return nil
}

func (writer *mmapBlockWriter) writeBlock(seq blockSeq, block *block) (blockSeq, blockSeq, error) {
	var buf []byte
	if writer.blockCompressed {
		buf = writer.compressBlock(block)
	} else {
		buf = writer.marshalBlock(block)
	}
	newSeq, err := writer.diskManager.WriteBuf(uint64(seq), buf)
	if err != nil {
		return 0, 0, err
	}
	return blockSeq(newSeq), blockSeq(newSeq) + blockSeq(len(buf)), nil
}

func (writer *mmapBlockWriter) compressBlock(block *block) []byte {
	stream := writer.stream
	stream.Reset(nil)
	stream.Marshal(*block)
	if stream.Error != nil {
		panic(stream.Error)
	}
	buf := stream.Buffer()
	compressBound := lz4.CompressBound(len(buf)) + int(compressedBlockHeaderSize)
	if compressBound > len(writer.blockCompressTmp) {
		writer.blockCompressTmp = make([]byte, compressBound)
	}
	compressedSize := lz4.CompressDefault(buf, writer.blockCompressTmp[compressedBlockHeaderSize:])
	stream.Reset(writer.blockCompressTmp[:0])
	stream.Marshal(compressedBlockHeader{
		blockOriginalSize:   uint32(len(buf)),
		blockCompressedSize: uint32(compressedSize),
	})
	if stream.Error != nil {
		panic(stream.Error)
	}
	return writer.blockCompressTmp[:compressedSize+int(compressedBlockHeaderSize)]
}

func (writer *mmapBlockWriter) marshalBlock(block *block) []byte {
	stream := writer.stream
	stream.Reset(nil)
	stream.Marshal(*block)
	if stream.Error != nil {
		panic(stream.Error)
	}
	return stream.Buffer()
}

func (writer *mmapBlockWriter) remove(untilSeq blockSeq) {
	writer.diskManager.Remove(uint64(untilSeq))
}

func (reader *mmapBlockReader) Close() error {
	return plz.Close(reader.memoryManager)
}

func (reader *mmapBlockReader) readBlock(seq blockSeq) (blk *block, err error) {
	if reader.blockCompressed {
		blk, err = reader.uncompressBlock(seq)
	} else {
		blk, err = reader.unmarshalBlock(seq)
	}
	if err != nil {
		return nil, err
	}
	return blk, nil
}

func (reader *mmapBlockReader) uncompressBlock(seq blockSeq) (*block, error) {
	headerBuf, err := reader.diskManager.ReadBuf(uint64(seq), compressedBlockHeaderSize)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(headerBuf)
	compressedBlockHeader, _ := iter.Unmarshal((*compressedBlockHeader)(nil)).(*compressedBlockHeader)
	if iter.Error != nil {
		return nil, fmt.Errorf("unmarshal compressedBlockHeader failed: %s", iter.Error)
	}
	decompressed := make([]byte, compressedBlockHeader.blockOriginalSize)
	compressedBuf, err := reader.diskManager.ReadBuf(
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

func (reader *mmapBlockReader) unmarshalBlock(seq blockSeq) (*block, error) {
	sizeBuf, err := reader.diskManager.ReadBuf(uint64(seq), 8)
	if err != nil {
		return nil, err
	}
	size := (*uint64)(unsafe.Pointer(&sizeBuf[0]))
	buf, err := reader.diskManager.ReadBuf(uint64(seq), uint32(*size))
	if err != nil {
		return nil, err
	}
	blk, err := reader.unmarshalBuf(seq, buf)
	if err != nil {
		return nil, fmt.Errorf("unmarshal block failed: %s", err)
	}
	return blk, nil
}

func (reader *mmapBlockReader) unmarshalBuf(seq blockSeq, buf []byte) (*block, error) {
	obj := reader.cache[seq]
	if obj != nil {
		return obj, nil
	}
	reader.iter.ObjectSeq(gocodec.ObjectSeq(seq))
	reader.iter.Reset(buf)
	obj, _ = reader.iter.Unmarshal((*block)(nil)).(*block)
	if reader.iter.Error != nil {
		return nil, reader.iter.Error
	}
	reader.cache[seq] = obj
	return obj, nil
}

func (reader *mmapBlockReader) gc() {
	reader.memoryManager.GC(func(seq gocodec.ObjectSeq) {
		delete(reader.cache, blockSeq(seq))
	})
}
