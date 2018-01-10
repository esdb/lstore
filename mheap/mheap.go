package mheap

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"github.com/esdb/gocodec"
)

type pageSeq uint64

// MemoryManager is not thread safe
type MemoryManager struct {
	startSeq     gocodec.ObjectSeq
	oldPages     []mmap.MMap
	oldPageRefs  [][]gocodec.ObjectSeq
	lastPage     mmap.MMap
	lastPageRefs []gocodec.ObjectSeq
	lastPageRef  gocodec.ObjectSeq
	// TODO: make sure objects can release memory when delete()
	objects              map[gocodec.ObjectSeq]interface{}
	pageSizeInPowerOfTwo uint8 // 2 ^ x
	pageSize             gocodec.ObjectSeq
	maxPagesCount        int
}

func New(pageSizeInPowerOfTwo uint8, maxPagesCount int) *MemoryManager {
	return &MemoryManager{
		pageSizeInPowerOfTwo: pageSizeInPowerOfTwo,
		pageSize:             1 << pageSizeInPowerOfTwo,
		pages:                []mmap.MMap{},
	}
}

func (mgr *MemoryManager) Close() error {
	var errs []error
	for _, page := range mgr.pages {
		err := page.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DiskManager.failed to close writeMMap", "err", err)
		}
	}
	return plz.MergeErrors(errs...)
}

func (mgr *MemoryManager) Allocate(objectSeq gocodec.ObjectSeq, original []byte) []byte {
}
