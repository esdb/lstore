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
	dataManager *dataManager
	indexCache  *lru.ARCCache
}

func newSlotIndexManager(config *slotIndexManagerConfig) *slotIndexManager {
	if config.IndexFileSizeInPowerOfTwo == 0 {
		config.IndexFileSizeInPowerOfTwo = 30
	}
	if config.IndexCacheSize == 0 {
		config.IndexCacheSize = 1024
	}
	indexCache, _ := lru.NewARC(config.IndexCacheSize)
	return &slotIndexManager{
		indexCache:  indexCache,
		dataManager: newDataManager(config.IndexDirectory, config.IndexFileSizeInPowerOfTwo),
	}
}

func (mgr *slotIndexManager) Close() error {
	return mgr.dataManager.Close()
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

func (mgr *slotIndexManager) readSlotIndex(seq slotIndexSeq) (*slotIndex, error) {
	header, err := mgr.dataManager.readBuf(uint64(seq), 4)
	if err != nil {
		return nil, err
	}
	size := *(*uint32)(unsafe.Pointer(&header[0]))
	buf, err := mgr.dataManager.readBuf(uint64(seq)+4, size)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	idx, _ := iter.Unmarshal((*slotIndex)(nil)).(*slotIndex)
	if iter.Error != nil {
		return nil, iter.Error
	}
	return idx, nil
}
