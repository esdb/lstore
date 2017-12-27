package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/concurrent"
	"context"
	"path"
	"github.com/esdb/lstore/ref"
)

const TailSegmentFileName = "tail.segment"

type Config struct {
	Directory           string
	CommandQueueSize    int
	TailSegmentMaxSize  int64
	CompactAfterStartup bool
}

func (conf *Config) TailSegmentPath() string {
	return path.Join(conf.Directory, TailSegmentFileName)
}

// Store is physically a directory, containing multiple files on disk
// it represents the history by a log of entries
type Store struct {
	Config
	*writer
	*compacter
	currentVersion unsafe.Pointer
	executor       *concurrent.UnboundedExecutor // owns writer and compacter
}

// StoreVersion is a view on the directory, keeping handle to opened files to avoid file being deleted or moved
type StoreVersion struct {
	config      Config
	*ref.ReferenceCounted
	rawSegments []*RawSegment
	tailSegment *TailSegment
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
	store.executor = concurrent.NewUnboundedExecutor()
	writer, err := store.newWriter()
	if err != nil {
		return err
	}
	store.writer = writer
	store.compacter = store.newCompacter()
	return nil
}

func (store *Store) Stop(ctx context.Context) {
	store.executor.StopAndWait(ctx)
}

func (store *Store) latest() *StoreVersion {
	for {
		version := (*StoreVersion)(atomic.LoadPointer(&store.currentVersion))
		if version == nil {
			return nil
		}
		if version.Acquire() {
			return version
		}
	}
}

func (store *Store) isLatest(version *StoreVersion) bool {
	latestVersion := (*StoreVersion)(atomic.LoadPointer(&store.currentVersion))
	return latestVersion == version
}
