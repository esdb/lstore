package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/concurrent"
	"context"
	"path"
	"github.com/esdb/lstore/ref"
	"io"
)

const TailSegmentFileName = "tail.segment"
const TailSegmentTmpFileName = "tail.segment.tmp"
const RootIndexedSegmentFileName = "indexed.segment"
const RootIndexedSegmentTmpFileName = "indexed.segment.tmp"

type Config struct {
	blockManagerConfig
	Directory              string
	CommandQueueSize       int
	TailSegmentMaxSize     int64
	indexingStrategy       *indexingStrategy
}

func (conf *Config) TailSegmentPath() string {
	return path.Join(conf.Directory, TailSegmentFileName)
}

func (conf *Config) TailSegmentTmpPath() string {
	return path.Join(conf.Directory, TailSegmentTmpFileName)
}

func (conf *Config) RootIndexedSegmentPath() string {
	return path.Join(conf.Directory, RootIndexedSegmentFileName)
}

func (conf *Config) RootIndexedSegmentTmpPath() string {
	return path.Join(conf.Directory, RootIndexedSegmentTmpFileName)
}

// Store is physically a directory, containing multiple files on disk
// it represents the history by a log of entries
type Store struct {
	Config
	*writer
	*indexer
	*compacter
	blockManager   *blockManager
	currentVersion unsafe.Pointer
	executor       *concurrent.UnboundedExecutor // owns writer and indexer
}

// StoreVersion is a view on the directory, keeping handle to opened files to avoid file being deleted or moved
type StoreVersion struct {
	config             Config
	*ref.ReferenceCounted
	rootIndexedSegment *rootIndexedSegment
	rawSegments        []*rawSegment
	tailSegment        *TailSegment
}

func (version StoreVersion) edit() *EditingStoreVersion {
	return &EditingStoreVersion{
		StoreVersion{
			config:             version.config,
			rootIndexedSegment: version.rootIndexedSegment,
			rawSegments:        version.rawSegments,
			tailSegment:        version.tailSegment,
		},
	}
}

type EditingStoreVersion struct {
	StoreVersion
}

func (edt EditingStoreVersion) seal() *StoreVersion {
	version := &edt.StoreVersion
	if !version.tailSegment.Acquire() {
		panic("acquire reference counter should not fail during version rotation")
	}
	resources := []io.Closer{version.tailSegment}
	for _, rawSegment := range version.rawSegments {
		if !rawSegment.Acquire() {
			panic("acquire reference counter should not fail during version rotation")
		}
		resources = append(resources, rawSegment)
	}
	if version.rootIndexedSegment != nil {
		if !version.rootIndexedSegment.Acquire() {
			panic("acquire reference counter should not fail during version rotation")
		}
		resources = append(resources, version.rootIndexedSegment)
	}
	version.ReferenceCounted = ref.NewReferenceCounted("store version", resources...)
	return version
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
	if store.BlockDirectory == "" {
		store.BlockDirectory = store.Directory + "/block"
	}
	store.blockManager = newBlockManager(&store.blockManagerConfig)
	store.Config.indexingStrategy = store.blockManager.indexingStrategy
	store.executor = concurrent.NewUnboundedExecutor()
	writer, err := store.newWriter()
	if err != nil {
		return err
	}
	store.writer = writer
	store.indexer = store.newIndexer()
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
