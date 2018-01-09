package lstore

import (
	"github.com/esdb/gocodec"
	"fmt"
	"github.com/esdb/lstore/dheap"
)

type slotIndexManagerConfig struct {
	IndexDirectory            string
	IndexFileSizeInPowerOfTwo uint8
}

type slotIndexManager interface {
	newSlotIndex(seq slotIndexSeq, level level) (slotIndexSeq, slotIndexSeq, *slotIndex, error)
	mapWritableSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error)
	readSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error)
	indexingStrategy() *IndexingStrategy
}

// slotIndexManager is global per store
// it manages the read/write to index file
// compress/decompress index from the mmap
// retain lru index cache
type mmapSlotIndexManager struct {
	strategy       *IndexingStrategy
	slotIndexSizes []uint32
	dataManager    *dheap.DataManager
}

func newSlotIndexManager(config *slotIndexManagerConfig, strategy *IndexingStrategy) *mmapSlotIndexManager {
	if config.IndexFileSizeInPowerOfTwo == 0 {
		config.IndexFileSizeInPowerOfTwo = 30
	}
	return &mmapSlotIndexManager{
		strategy:       strategy,
		slotIndexSizes: make([]uint32, levelsCount),
		dataManager:    dheap.New(config.IndexDirectory, config.IndexFileSizeInPowerOfTwo),
	}
}

func (mgr *mmapSlotIndexManager) Close() error {
	return mgr.dataManager.Close()
}

func (mgr *mmapSlotIndexManager) indexingStrategy() *IndexingStrategy {
	return mgr.strategy
}

func (mgr *mmapSlotIndexManager) newSlotIndex(seq slotIndexSeq, level level) (slotIndexSeq, slotIndexSeq, *slotIndex, error) {
	size := mgr.getSlotIndexSize(level)
	newSeq, buf, err := mgr.dataManager.AllocateBuf(uint64(seq), size)
	if err != nil {
		return 0, 0, nil, err
	}
	obj := newSlotIndex(mgr.strategy, level)
	stream := gocodec.NewStream(buf[:0])
	stream.Marshal(*obj)
	if stream.Error != nil {
		return 0, 0, nil, stream.Error
	}
	iter := gocodec.ReadonlyConfig.NewIterator(buf)
	// re-construct the object on the mmap heap
	obj, _ = iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return 0, 0, nil, fmt.Errorf("newSlotIndex failed: %s", iter.Error.Error())
	}
	return slotIndexSeq(newSeq), slotIndexSeq(newSeq) + slotIndexSeq(size), obj, nil
}

func (mgr *mmapSlotIndexManager) getSlotIndexSize(level level) uint32 {
	size := mgr.slotIndexSizes[level]
	if size == 0 {
		buf, err := gocodec.Marshal(*newSlotIndex(mgr.strategy, level))
		if err != nil {
			panic(err)
		}
		size = uint32(len(buf))
		mgr.slotIndexSizes[level] = size
	}
	return size
}

func (mgr *mmapSlotIndexManager) mapWritableSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error) {
	size := mgr.getSlotIndexSize(level)
	buf, err := mgr.dataManager.MapWritableBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	iter := gocodec.ReadonlyConfig.NewIterator(buf)
	idx, _ := iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return nil, fmt.Errorf("mapWritableSlotIndex failed: %s", iter.Error.Error())
	}
	return idx, nil
}

func (mgr *mmapSlotIndexManager) readSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error) {
	size := mgr.getSlotIndexSize(level)
	buf, err := mgr.dataManager.ReadBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	iter := gocodec.ReadonlyConfig.NewIterator(buf)
	idx, _ := iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return nil, fmt.Errorf("readSlotIndex failed: %s", iter.Error.Error())
	}
	return idx, nil
}

func (mgr *mmapSlotIndexManager) remove(untilSeq slotIndexSeq) {
	mgr.dataManager.Remove(uint64(untilSeq))
}
