package lstore

import (
	"github.com/hashicorp/golang-lru"
	"github.com/esdb/gocodec"
	"unsafe"
)

type slotIndexManagerConfig struct {
	IndexDirectory            string
	IndexFileSizeInPowerOfTwo uint8
	IndexCacheSize            int
}

// slotIndexManager is global per store
// it manages the read/write to index file
// compress/decompress index from the mmap
// retain lru index cache
type slotIndexManager struct {
	strategy       *IndexingStrategy
	slotIndexSizes []uint32
	dataManager    *dataManager
	indexCache     *lru.ARCCache
}

func newSlotIndexManager(config *slotIndexManagerConfig, strategy *IndexingStrategy) *slotIndexManager {
	if config.IndexFileSizeInPowerOfTwo == 0 {
		config.IndexFileSizeInPowerOfTwo = 30
	}
	if config.IndexCacheSize == 0 {
		config.IndexCacheSize = 1024
	}
	indexCache, _ := lru.NewARC(config.IndexCacheSize)
	return &slotIndexManager{
		strategy:       strategy,
		slotIndexSizes: make([]uint32, levelsCount),
		indexCache:     indexCache,
		dataManager:    newDataManager(config.IndexDirectory, config.IndexFileSizeInPowerOfTwo),
	}
}

func (mgr *slotIndexManager) Close() error {
	return mgr.dataManager.Close()
}

func (mgr *slotIndexManager) newSlotIndex(seq slotIndexSeq, level level) (slotIndexSeq, slotIndexSeq, *slotIndex, error) {
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
		return 0, 0, nil, iter.Error
	}
	return slotIndexSeq(newSeq), slotIndexSeq(newSeq) + slotIndexSeq(size), obj, nil
}

func (mgr *slotIndexManager) getSlotIndexSize(level level) uint32 {
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

func (mgr *slotIndexManager) writeSlotIndex(seq slotIndexSeq, idx *slotIndex) (slotIndexSeq, error) {
	stream := gocodec.NewStream(nil)
	stream.Reset(nil)
	size := uint32(stream.Marshal(*idx))
	if stream.Error != nil {
		return 0, stream.Error
	}
	header := (*(*[4]byte)(unsafe.Pointer(&size)))[:]
	_, err := mgr.dataManager.writeBuf(uint64(seq), header)
	if err != nil {
		return 0, err
	}
	_, err = mgr.dataManager.writeBuf(uint64(seq)+4, stream.Buffer())
	if err != nil {
		return 0, err
	}
	return seq + slotIndexSeq(size) + 4, nil
}

func (mgr *slotIndexManager) mapWritableSlotIndex(seq slotIndexSeq, level level) (*slotIndex, error) {
	size := mgr.getSlotIndexSize(level)
	buf, err := mgr.dataManager.mapWritableBuf(uint64(seq), size)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	idx, _ := iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return nil, iter.Error
	}
	gocodec.UpdateChecksum(buf)
	return idx, nil
}

func (mgr *slotIndexManager) flush(seq slotIndexSeq, level level) error {
	buf, err := mgr.dataManager.mapWritableBuf(uint64(seq), mgr.getSlotIndexSize(level))
	if err != nil {
		return err
	}
	gocodec.UpdateChecksum(buf)
	return mgr.dataManager.flush(uint64(seq))
}
