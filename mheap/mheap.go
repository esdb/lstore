package mheap

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"github.com/esdb/gocodec"
	"math"
)

// MemoryManager is not thread safe
type MemoryManager struct {
	startSeq     gocodec.ObjectSeq
	oldPages     []mmap.MMap
	oldPageRefs  [][]gocodec.ObjectSeq
	lastPage     mmap.MMap
	lastPageBuf  []byte
	lastPageRefs []gocodec.ObjectSeq
	lastPageRef  gocodec.ObjectSeq
	// TODO: make sure objects can release memory when delete()
	objects              map[gocodec.ObjectSeq]interface{}
	pageSizeInPowerOfTwo uint8 // 2 ^ x
	pageSize             int
	maxOldPagesCount     int
	iter                 *gocodec.Iterator
}

func New(pageSizeInPowerOfTwo uint8, maxPagesCount int) *MemoryManager {
	lastPage, err := mmap.MapRegion(nil, 1<<pageSizeInPowerOfTwo, mmap.RDWR, mmap.ANON, 0)
	countlog.TraceCall("callee!mmap.MapRegion", err)
	if err != nil {
		panic(err)
	}
	iter := gocodec.ReadonlyConfig.NewIterator(nil)
	mgr := &MemoryManager{
		pageSizeInPowerOfTwo: pageSizeInPowerOfTwo,
		pageSize:             1 << pageSizeInPowerOfTwo,
		maxOldPagesCount:     maxPagesCount - 1,
		lastPage:             lastPage,
		lastPageBuf:          lastPage[:],
		lastPageRef:          math.MaxUint64,
		iter:                 iter,
		objects:              map[gocodec.ObjectSeq]interface{}{},
	}
	iter.Allocator(mgr)
	return mgr
}

func (mgr *MemoryManager) Close() error {
	var errs []error
	for _, page := range append(mgr.oldPages, mgr.lastPage) {
		err := page.Unmap()
		countlog.TraceCall("callee!page.Unmap", err)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return plz.MergeErrors(errs...)
}

func (mgr *MemoryManager) Allocate(objectSeq gocodec.ObjectSeq, original []byte) []byte {
	size := len(original)
	if size > len(mgr.lastPageBuf) {
		if size > mgr.pageSize {
			countlog.Fatal("event!mheap.page size is too small",
				"size", size,
				"pageSize", mgr.pageSize)
			panic("page size is too small")
		}
		newPage, err := mmap.MapRegion(nil, mgr.pageSize, mmap.RDWR, mmap.ANON, 0)
		countlog.TraceCall("callee!mmap.MapRegion", err)
		if err != nil {
			panic(err)
		}
		mgr.oldPageRefs = append(mgr.oldPageRefs, mgr.lastPageRefs)
		mgr.oldPages = append(mgr.oldPages, mgr.lastPage)
		mgr.lastPage = newPage
		mgr.lastPageBuf = newPage[:]
		if len(mgr.oldPages) > mgr.maxOldPagesCount {
			expiresCount := len(mgr.oldPages) - mgr.maxOldPagesCount
			expiredPages := mgr.oldPages[:expiresCount]
			for i, page := range expiredPages {
				for _, refObjectSeq := range mgr.oldPageRefs[i] {
					delete(mgr.objects, refObjectSeq)
				}
				err = page.Unmap()
				countlog.TraceCall("callee!page.Unmap", err)
			}
			mgr.lastPageRefs = mgr.lastPageRefs[expiresCount:]
			mgr.oldPages = mgr.oldPages[expiresCount:]
		}
	}
	allocated := mgr.lastPageBuf[:size]
	mgr.lastPageBuf = mgr.lastPageBuf[size:]
	copy(allocated, original)
	if mgr.lastPageRef != objectSeq {
		mgr.lastPageRef = objectSeq
		mgr.lastPageRefs = append(mgr.lastPageRefs, objectSeq)
	}
	return allocated
}

func (mgr *MemoryManager) Unmarshal(objectSeq gocodec.ObjectSeq, buf []byte, candidatePointer interface{}) (interface{}, error) {
	obj := mgr.objects[objectSeq]
	if obj != nil {
		return obj, nil
	}
	mgr.iter.ObjectSeq(objectSeq)
	mgr.iter.Reset(buf)
	obj = mgr.iter.Unmarshal(candidatePointer)
	if mgr.iter.Error != nil {
		return nil, mgr.iter.Error
	}
	mgr.objects[objectSeq] = obj
	return obj, nil
}
