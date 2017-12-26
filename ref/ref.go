package ref

import (
	"io"
	"github.com/v2pro/plz/countlog"
	"sync/atomic"
)

type MultiError []error

func (errs MultiError) Error() string {
	return "multiple errors"
}

func NewMultiError(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	return MultiError(errs)
}

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
	countlog.Info("event!ref.close reference counted resource", "resourceName", refCnt.resourceName)
	var errs []error
	for _, res := range refCnt.resources {
		if err := res.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return NewMultiError(errs)
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

type funcResource struct {
	resourceName string
	f            func() error
}

func (res *funcResource) Close() error {
	err := res.f()
	if err != nil {
		countlog.Error("event!ref.failed to close resource",
			"resourceName", res.resourceName, "err", err)
		return err
	}
	return nil
}

func NewResource(resourceName string, f func() error) io.Closer {
	return &funcResource{resourceName, f}
}