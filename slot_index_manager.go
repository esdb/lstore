package lstore

import (
	"github.com/hashicorp/golang-lru"
	"sync"
	"github.com/edsrzf/mmap-go"
	"os"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/lstore/ref"
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
	indexDirectory            string
	indexFileSizeInPowerOfTwo uint8 // 2 ^ x
	indexFileSize             uint64
	indexCache                *lru.ARCCache
	// only lock the modification of following maps
	// does not cover reading or writing
	mapMutex   *sync.Mutex
	files      map[slotIndexSeq]*os.File
	writeMMaps map[slotIndexSeq]mmap.MMap
	readMMaps  map[slotIndexSeq]mmap.MMap
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
		indexDirectory:            config.IndexDirectory,
		indexFileSizeInPowerOfTwo: config.IndexFileSizeInPowerOfTwo,
		indexFileSize:             2 << config.IndexFileSizeInPowerOfTwo,
		indexCache:                indexCache,
		mapMutex:                  &sync.Mutex{},
		files:                     map[slotIndexSeq]*os.File{},
		readMMaps:                 map[slotIndexSeq]mmap.MMap{},
		writeMMaps:                map[slotIndexSeq]mmap.MMap{},
	}
}

func (mgr *slotIndexManager) Close() error {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	var errs []error
	for _, writeMMap := range mgr.writeMMaps {
		err := writeMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!slotIndexManager.failed to close writeMMap", "err", err)
		}
	}
	for _, readMMap := range mgr.readMMaps {
		err := readMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!slotIndexManager.failed to close readMMap", "err", err)
		}
	}
	for _, file := range mgr.files {
		err := file.Close()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!slotIndexManager.failed to close file", "err", err)
		}
	}
	return ref.NewMultiError(errs)
}

func (mgr *slotIndexManager) writeSlotIndex(seq slotIndexSeq, idx *slotIndex) (slotIndexSeq, error) {
	return 0, nil
}

func (mgr *slotIndexManager) readSlotIndex(seq slotIndexSeq) (*slotIndex, error) {
	return nil, nil
}
