package lstore

import (
	"github.com/esdb/gocodec"
	"fmt"
	"github.com/esdb/lstore/dheap"
	"github.com/esdb/lstore/mheap"
	"io"
	"github.com/v2pro/plz"
)

type slotIndexManagerConfig struct {
	IndexDirectory            string
	IndexFileSizeInPowerOfTwo uint8
}

type slotIndexManager interface {
	io.Closer
	newReader(pageSizeInPowerOfTwo uint8, maxPagesCount int) slotIndexReader
	newWriter(pageSizeInPowerOfTwo uint8, maxPagesCount int) slotIndexWriter
}

type slotIndexReader interface {
	io.Closer
	readSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error)
	gc()
}

type slotIndexWriter interface {
	io.Closer
	newSlotIndex(seq slotIndexSeq, level level) (slotIndexSeq, slotIndexSeq, *slotIndex, error)
	mapWritableSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error)
	gc()
	indexingStrategy() *indexingStrategy
	remove(untilSeq slotIndexSeq)
}

// slotIndexManager is global per store
// it manages the read/write to index file
// compress/decompress index from the mmap
// retain lru index cache
type mmapSlotIndexManager struct {
	strategy       *indexingStrategy
	slotIndexSizes []uint32
	diskManager    *dheap.DiskManager
}

type mmapSlotIndexReader struct {
	mmapSlotIndexManager
	memoryManager *mheap.MemoryManager
	cache         map[slotIndexSeq]*slotIndex
	iter          *gocodec.Iterator
}

type mmapSlotIndexWriter struct {
	mmapSlotIndexManager
	stream        *gocodec.Stream
	memoryManager *mheap.MemoryManager
	cache         map[slotIndexSeq]*slotIndex
	iter          *gocodec.Iterator
}

func newSlotIndexManager(config *slotIndexManagerConfig, strategy *indexingStrategy) *mmapSlotIndexManager {
	if config.IndexFileSizeInPowerOfTwo == 0 {
		config.IndexFileSizeInPowerOfTwo = 30
	}
	slotIndexSizes := make([]uint32, levelsCount)
	for i := level0; i < levelsCount; i++ {
		buf, err := gocodec.Marshal(*newSlotIndex(strategy, i))
		if err != nil {
			panic(err)
		}
		size := uint32(len(buf))
		slotIndexSizes[i] = size
	}
	return &mmapSlotIndexManager{
		strategy:       strategy,
		slotIndexSizes: slotIndexSizes,
		diskManager:    dheap.New(config.IndexDirectory, config.IndexFileSizeInPowerOfTwo),
	}
}

func (mgr *mmapSlotIndexManager) Close() error {
	return plz.Close(mgr.diskManager)
}

func (mgr *mmapSlotIndexManager) newReader(pageSizeInPowerOfTwo uint8, maxPagesCount int) slotIndexReader {
	memMgr := mheap.New(pageSizeInPowerOfTwo, maxPagesCount)
	iter := gocodec.ReadonlyConfig.NewIterator(nil)
	iter.Allocator(memMgr)
	return &mmapSlotIndexReader{
		mmapSlotIndexManager: *mgr,
		memoryManager:        memMgr,
		iter:                 iter,
		cache:                map[slotIndexSeq]*slotIndex{},
	}
}

func (mgr *mmapSlotIndexManager) newWriter(pageSizeInPowerOfTwo uint8, maxPagesCount int) slotIndexWriter {
	memMgr := mheap.New(pageSizeInPowerOfTwo, maxPagesCount)
	iter := gocodec.ReadonlyConfig.NewIterator(nil)
	iter.Allocator(memMgr)
	return &mmapSlotIndexWriter{
		mmapSlotIndexManager: *mgr,
		memoryManager:        memMgr,
		stream:               gocodec.NewStream(nil),
		iter:                 iter,
		cache:                map[slotIndexSeq]*slotIndex{},
	}
}

func (writer *mmapSlotIndexWriter) Close() error {
	return plz.Close(writer.memoryManager)
}

func (writer *mmapSlotIndexWriter) indexingStrategy() *indexingStrategy {
	return writer.strategy
}

func (writer *mmapSlotIndexWriter) newSlotIndex(seq slotIndexSeq, level level) (slotIndexSeq, slotIndexSeq, *slotIndex, error) {
	size := writer.slotIndexSizes[level]
	newSeq, buf, err := writer.diskManager.AllocateBuf(uint64(seq), size)
	if err != nil {
		return 0, 0, nil, err
	}
	obj := newSlotIndex(writer.strategy, level)
	stream := writer.stream
	stream.Reset(buf[:0])
	stream.Marshal(*obj)
	if stream.Error != nil {
		return 0, 0, nil, stream.Error
	}
	// re-construct the object on the mmap heap
	obj, err = writer.unmarshalBuf(slotIndexSeq(newSeq), buf)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("newSlotIndex failed: %s", err)
	}
	return slotIndexSeq(newSeq), slotIndexSeq(newSeq) + slotIndexSeq(size), obj, nil
}

func (writer *mmapSlotIndexWriter) mapWritableSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error) {
	size := writer.slotIndexSizes[level]
	buf, err := writer.diskManager.MapWritableBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	idx, err := writer.unmarshalBuf(seq, buf)
	if err != nil {
		return nil, fmt.Errorf("mapWritableSlotIndex failed: %s", err)
	}
	return idx, nil
}

func (writer *mmapSlotIndexWriter) remove(untilSeq slotIndexSeq) {
	writer.diskManager.Remove(uint64(untilSeq))
}

func (writer *mmapSlotIndexWriter) gc() {
	writer.memoryManager.GC(func(seq gocodec.ObjectSeq) {
		delete(writer.cache, slotIndexSeq(seq))
	})
}

func (writer *mmapSlotIndexWriter) unmarshalBuf(seq slotIndexSeq, buf []byte) (*slotIndex, error) {
	obj := writer.cache[seq]
	if obj != nil {
		return obj, nil
	}
	writer.iter.ObjectSeq(gocodec.ObjectSeq(seq))
	writer.iter.Reset(buf)
	obj, _ = writer.iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if writer.iter.Error != nil {
		return nil, writer.iter.Error
	}
	writer.cache[seq] = obj
	return obj, nil
}

func (reader *mmapSlotIndexReader) Close() error {
	return plz.Close(reader.memoryManager)
}

func (reader *mmapSlotIndexReader) readSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error) {
	size := reader.slotIndexSizes[level]
	buf, err := reader.diskManager.ReadBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	idx, err := reader.unmarshalBuf(seq, buf)
	if err != nil {
		return nil, fmt.Errorf("readSlotIndex failed: %s", err)
	}
	return idx, nil
}

func (reader *mmapSlotIndexReader) gc() {
	reader.memoryManager.GC(func(seq gocodec.ObjectSeq) {
		delete(reader.cache, slotIndexSeq(seq))
	})
}

func (reader *mmapSlotIndexReader) unmarshalBuf(seq slotIndexSeq, buf []byte) (*slotIndex, error) {
	obj := reader.cache[seq]
	if obj != nil {
		return obj, nil
	}
	reader.iter.ObjectSeq(gocodec.ObjectSeq(seq))
	reader.iter.Reset(buf)
	obj, _ = reader.iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if reader.iter.Error != nil {
		return nil, reader.iter.Error
	}
	reader.cache[seq] = obj
	return obj, nil
}
