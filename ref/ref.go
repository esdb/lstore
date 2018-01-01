package ref

import (
	"io"
	"github.com/v2pro/plz/countlog"
	"sync/atomic"
	"github.com/v2pro/plz"
)

type ReferenceCounted struct {
	resourceName     string
	referenceCounter uint32
	resources        []io.Closer
}

func NewReferenceCounted(resourceName string, resources ...io.Closer) *ReferenceCounted {
	return &ReferenceCounted{resourceName: resourceName, referenceCounter: 1, resources: resources}
}

func (refCnt *ReferenceCounted) Acquire() bool {
	for {
		counter := atomic.LoadUint32(&refCnt.referenceCounter)
		if counter == 0 {
			// already disposed, can not be used
			return false
		}
		if !atomic.CompareAndSwapUint32(&refCnt.referenceCounter, counter, counter+1) {
			// retry
			continue
		}
		return true
	}
}

func (refCnt *ReferenceCounted) Close() error {
	if !refCnt.decreaseReference() {
		return nil // still in use
	}
	countlog.Trace("event!ref.close reference counted resource", "resourceName", refCnt.resourceName)
	var errs []error
	for _, res := range refCnt.resources {
		if err := res.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return plz.MergeErrors(errs...)
}

func (refCnt *ReferenceCounted) decreaseReference() bool {
	for {
		counter := atomic.LoadUint32(&refCnt.referenceCounter)
		if counter == 0 {
			return true
		}
		if atomic.CompareAndSwapUint32(&refCnt.referenceCounter, counter, counter-1) {
			return counter == 1 // last one should close the currentVersion
		}
	}
}