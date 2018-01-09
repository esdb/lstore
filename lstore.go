package lstore

import (
	"unsafe"
	"sync/atomic"
	"github.com/v2pro/plz/concurrent"
	"path"
	"fmt"
	"context"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
)

const IndexingSegmentFileName = "indexing.segment"
const IndexingSegmentTmpFileName = "indexing.segment.tmp"
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

func (conf *Config) IndexedSegmentPath(tailOffset Offset) string {
	return path.Join(conf.Directory, fmt.Sprintf("indexed-%d.segment", tailOffset))
}

func (conf *Config) IndexingSegmentPath() string {
	return path.Join(conf.Directory, IndexingSegmentFileName)
}

func (conf *Config) IndexingSegmentTmpPath() string {
	return path.Join(conf.Directory, IndexingSegmentTmpFileName)
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
	writer           *writer
	indexer          *indexer
	blockManager     *mmapBlockManager
	slotIndexManager *mmapSlotIndexManager
	currentVersion   unsafe.Pointer                // pointer to StoreVersion, writer use atomic to notify readers
	tailOffset       uint64                        // offset, writer use atomic to notify readers
	executor         *concurrent.UnboundedExecutor // owns writer and indexer
}

type StoreVersion struct {
	indexedChunks []*indexChunk
	indexingChunk *indexChunk
	rawChunks     []*rawChunk
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
	if store.IndexingStrategy == nil {
		store.IndexingStrategy = NewIndexingStrategy(IndexingStrategyConfig{})
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

func (store *Store) Write(ctxObj context.Context, entry *Entry) (Offset, error) {
	return store.writer.Write(ctxObj, entry)
}

func (store *Store) UpdateIndex() error {
	return store.indexer.UpdateIndex()
}

func (store *Store) RotateIndex() error {
	return store.indexer.RotateIndex()
}

// Remove can only remove those rows in indexed segment
// data still hanging in indexing/raw/tail segment can not be removed
func (store *Store) Remove(untilOffset Offset) error {
	return store.indexer.Remove(untilOffset)
}

func (store *Store) latest() *StoreVersion {
	for {
		version := (*StoreVersion)(atomic.LoadPointer(&store.currentVersion))
		return version
	}
}

func (store *Store) isLatest(version *StoreVersion) bool {
	latestVersion := (*StoreVersion)(atomic.LoadPointer(&store.currentVersion))
	return latestVersion == version
}

func (store *Store) getTailOffset() Offset {
	return Offset(atomic.LoadUint64(&store.tailOffset))
}
