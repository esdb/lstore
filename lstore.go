package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/concurrent"
	"context"
	"path"
)

const TailSegmentFileName = "tail.segment"

type Config struct {
	Directory          string
	CommandQueueSize   int
	TailSegmentMaxSize int64
}

func (conf *Config) TailSegmentPath() string {
	return path.Join(conf.Directory, TailSegmentFileName)
}

type Store struct {
	Config
	*Writer
	currentVersion unsafe.Pointer
	executor       *concurrent.UnboundedExecutor // owns writer and compacter
}

type StoreVersion struct {
	config           Config
	referenceCounter uint32
	rawSegments      []*RawSegment
	tailSegment      *TailSegment
}

func (store *Store) Start() error {
	if store.CommandQueueSize == 0 {
		store.CommandQueueSize = 1024
	}
	if store.Directory == "" {
		store.Directory = "/tmp"
	}
	if store.TailSegmentMaxSize == 0 {
		store.TailSegmentMaxSize = 200 * 1024 * 1024
	}
	initialVersion, err := store.loadData()
	if err != nil {
		return err
	}
	atomic.StorePointer(&store.currentVersion, unsafe.Pointer(initialVersion))
	store.executor = concurrent.NewUnboundedExecutor()
	store.Writer = &Writer{make(chan Command, store.CommandQueueSize)}
	store.Writer.start(store, initialVersion)
	return nil
}

func (store *Store) Stop(ctx context.Context) {
	store.executor.StopAndWait(ctx)
}

func (store *Store) Latest() *StoreVersion {
	for {
		store := (*StoreVersion)(atomic.LoadPointer(&store.currentVersion))
		if store == nil {
			return nil
		}
		counter := atomic.LoadUint32(&store.referenceCounter)
		if counter == 0 {
			// retry
			continue
		}
		if !atomic.CompareAndSwapUint32(&store.referenceCounter, counter, counter+1) {
			// retry
			continue
		}
		return store
	}
}

func (version *StoreVersion) Close() error {
	if !version.decreaseReference() {
		return nil // still in use
	}
	return version.tailSegment.Close()
}

func (version *StoreVersion) decreaseReference() bool {
	for {
		counter := atomic.LoadUint32(&version.referenceCounter)
		if counter == 0 {
			return true
		}
		if atomic.CompareAndSwapUint32(&version.referenceCounter, counter, counter-1) {
			return counter == 1 // last one should close the currentVersion
		}
	}
}
