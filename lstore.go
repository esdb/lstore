package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/concurrent"
	"context"
	"path"
	"fmt"
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
	currentVersion unsafe.Pointer
	commandQueue   chan Command
	executor       *concurrent.UnboundedExecutor
}

type Command func(ctx context.Context, store *StoreVersion) *StoreVersion

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
	store.commandQueue = make(chan Command, store.CommandQueueSize)
	initialVersion, err := store.loadData()
	if err != nil {
		return err
	}
	atomic.StorePointer(&store.currentVersion, unsafe.Pointer(initialVersion))
	store.startCommandQueue(initialVersion)
	return nil
}

func (store *Store) loadData() (*StoreVersion, error) {
	tailSegment, err := openTailSegment(store.TailSegmentPath(), store.TailSegmentMaxSize, 0)
	if err != nil {
		return nil, err
	}
	var reversedRawSegments []*RawSegment
	startOffset := tailSegment.StartOffset
	for startOffset != 0 {
		prev := path.Join(store.Directory, fmt.Sprintf("%d.segment", startOffset))
		rawSegment, err := openRawSegment(prev, startOffset)
		if err != nil {
			return nil, err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		startOffset = rawSegment.StartOffset
	}
	rawSegments := make([]*RawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	return &StoreVersion{
		referenceCounter: 1,
		config:           store.Config,
		rawSegments:      rawSegments,
		tailSegment:      tailSegment,
	}, nil
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

func (version *StoreVersion) TailSegment() *TailSegment {
	return version.tailSegment
}

func (version *StoreVersion) RawSegments() []*RawSegment {
	return version.rawSegments
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
