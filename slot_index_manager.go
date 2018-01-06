package lstore

import (
	"github.com/hashicorp/golang-lru"
	"github.com/esdb/gocodec"
	"fmt"
)

type slotIndexManagerConfig struct {
	IndexDirectory            string
	IndexFileSizeInPowerOfTwo uint8
	IndexCacheSize            int
}

type slotIndexManager interface {
	newSlotIndex(seq slotIndexSeq, level level) (slotIndexSeq, slotIndexSeq, *slotIndex, error)
	mapWritableSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error)
	readSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error)
	updateChecksum(seq slotIndexSeq, level level) error
	indexingStrategy() *IndexingStrategy
}

// slotIndexManager is global per store
// it manages the read/write to index file
// compress/decompress index from the mmap
// retain lru index cache
type mmapSlotIndexManager struct {
	strategy       *IndexingStrategy
	slotIndexSizes []uint32
	dataManager    *dataManager
	rwCache        *lru.ARCCache
	roCache        *lru.ARCCache
}

func newSlotIndexManager(config *slotIndexManagerConfig, strategy *IndexingStrategy) *mmapSlotIndexManager {
	if config.IndexFileSizeInPowerOfTwo == 0 {
		config.IndexFileSizeInPowerOfTwo = 30
	}
	if config.IndexCacheSize == 0 {
		config.IndexCacheSize = 1024
	}
	rwCache, _ := lru.NewARC(levelsCount)
	roCache, _ := lru.NewARC(config.IndexCacheSize)
	return &mmapSlotIndexManager{
		strategy:       strategy,
		slotIndexSizes: make([]uint32, levelsCount),
		rwCache:        rwCache,
		roCache:        roCache,
		dataManager:    newDataManager(config.IndexDirectory, config.IndexFileSizeInPowerOfTwo),
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
	newSeq, buf, err := mgr.dataManager.allocateBuf(uint64(seq), size)
	if err != nil {
		return 0, 0, nil, err
	}
	obj := newSlotIndex(mgr.strategy, level)
	stream := gocodec.NewStream(buf[:0])
	stream.Marshal(*obj)
	if stream.Error != nil {
		return 0, 0, nil, stream.Error
	}
	iter := gocodec.NewIterator(buf)
	// re-construct the object on the mmap heap
	obj, _ = iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return 0, 0, nil, fmt.Errorf("newSlotIndex failed: %s", iter.Error.Error())
	}
	gocodec.UpdateChecksum(buf)
	mgr.rwCache.Add(slotIndexSeq(newSeq), obj)
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
	cache, found := mgr.rwCache.Get(seq)
	if found {
		return cache.(*slotIndex), nil
	}
	size := mgr.getSlotIndexSize(level)
	buf, err := mgr.dataManager.mapWritableBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	idx, _ := iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return nil, fmt.Errorf("mapWritableSlotIndex failed: %s", iter.Error.Error())
	}
	gocodec.UpdateChecksum(buf)
	mgr.rwCache.Add(seq, idx)
	return idx, nil
}

func (mgr *mmapSlotIndexManager) readSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error) {
	cache, found := mgr.roCache.Get(seq)
	if found {
		return cache.(*slotIndex), nil
	}
	size := mgr.getSlotIndexSize(level)
	buf, err := mgr.dataManager.readBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	idx, _ := iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return nil, fmt.Errorf("readSlotIndex failed: %s", iter.Error.Error())
	}
	mgr.roCache.Add(seq, idx)
	return idx, nil
}

func (mgr *mmapSlotIndexManager) updateChecksum(seq slotIndexSeq, level level) error {
	buf, err := mgr.dataManager.mapWritableBuf(uint64(seq), mgr.getSlotIndexSize(level))
	if err != nil {
		return err
	}
	gocodec.UpdateChecksum(buf)
	return nil
}
