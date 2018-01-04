package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/concurrent"
	"path"
	"github.com/esdb/lstore/ref"
	"io"
	"fmt"
	"context"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
)

const IndexingSegmentFileName = "indexing.segment"
const TailSegmentFileName = "tail.segment"
const TailSegmentTmpFileName = "tail.segment.tmp"

type Config struct {
	blockManagerConfig
	slotIndexManagerConfig
	Directory          string
	CommandQueueSize   int
	TailSegmentMaxSize int64
	IndexingStrategy   *IndexingStrategy
}

func (conf *Config) IndexSegmentPath(tailOffset Offset) string {
	return path.Join(conf.Directory, fmt.Sprintf("index-%d.segment", tailOffset))
}

func (conf *Config) IndexingSegmentPath() string {
	return path.Join(conf.Directory, IndexingSegmentFileName)
}

func (conf *Config) RawSegmentPath(tailOffset Offset) string {
	return path.Join(conf.Directory, fmt.Sprintf("raw-%d.segment", tailOffset))
}

func (conf *Config) TailSegmentPath() string {
	return path.Join(conf.Directory, TailSegmentFileName)
}

func (conf *Config) TailSegmentTmpPath() string {
	return path.Join(conf.Directory, TailSegmentTmpFileName)
}

// Store is physically a directory, containing multiple files on disk
// it represents the history by a log of entries
type Store struct {
	Config
	*writer
	*indexer
	blockManager     *mmapBlockManager
	slotIndexManager *mmapSlotIndexManager
	currentVersion   unsafe.Pointer
	executor         *concurrent.UnboundedExecutor // owns writer and indexer
}

// StoreVersion is a view on the directory, keeping handle to opened files to avoid file being deleted or moved
type StoreVersion struct {
	config          Config
	*ref.ReferenceCounted
	indexingSegment *indexingSegment
	rawSegments     []*rawSegment
	tailSegment     *tailSegment
}

func (version StoreVersion) edit() *EditingStoreVersion {
	return &EditingStoreVersion{
		StoreVersion{
			config:          version.config,
			indexingSegment: version.indexingSegment,
			rawSegments:     version.rawSegments,
			tailSegment:     version.tailSegment,
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
	if version.indexingSegment != nil {
		if !version.indexingSegment.Acquire() {
			panic("acquire reference counter should not fail during version rotation")
		}
		resources = append(resources, version.indexingSegment)
	}
	version.ReferenceCounted = ref.NewReferenceCounted("store version", resources...)
	return version
}

func (store *Store) Start(ctxObj context.Context) error {
	ctx := countlog.Ctx(ctxObj)
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
		store.BlockDirectory = path.Join(store.Directory, "block")
	}
	if store.IndexDirectory == "" {
		store.IndexDirectory = path.Join(store.Directory, "index")
	}
	store.blockManager = newBlockManager(&store.blockManagerConfig)
	store.slotIndexManager = newSlotIndexManager(&store.slotIndexManagerConfig, store.IndexingStrategy)
	store.executor = concurrent.NewUnboundedExecutor()
	writer, err := store.newWriter(ctx)
	if err != nil {
		return err
	}
	store.writer = writer
	store.indexer = store.newIndexer(ctx)
	return nil
}

func (store *Store) Stop(ctx context.Context) error {
	store.executor.StopAndWait(ctx)
	return plz.Close(store.writer)
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
