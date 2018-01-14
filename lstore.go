package lstore

import (
	"github.com/v2pro/plz/concurrent"
	"path"
	"context"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"io"
)

type Config struct {
	writerConfig
	indexerConfig
	blockManagerConfig
	slotIndexManagerConfig
	indexingStrategyConfig
	Directory string
}

// Store is physically a directory, containing multiple files on disk
// it represents the history by a log of entries
type Store struct {
	storeState
	cfg      *Config
	writer   *writer
	indexer  *indexer
	strategy *indexingStrategy
	executor *concurrent.UnboundedExecutor // owns writer and indexer, 2 goroutines
}

func New(ctxObj context.Context, cfg *Config) (*Store, error) {
	ctx := countlog.Ctx(ctxObj)
	if cfg.Directory == "" {
		cfg.Directory = "/tmp/store"
	}
	if cfg.RawSegmentDirectory == "" {
		cfg.RawSegmentDirectory = cfg.Directory
	}
	if cfg.IndexSegmentDirectory == "" {
		cfg.IndexSegmentDirectory = cfg.Directory
	}
	if cfg.BlockDirectory == "" {
		cfg.BlockDirectory = path.Join(cfg.Directory, "block")
	}
	if cfg.IndexDirectory == "" {
		cfg.IndexDirectory = path.Join(cfg.Directory, "index")
	}
	strategy := newIndexingStrategy(&cfg.indexingStrategyConfig)
	store := &Store{
		cfg:      cfg,
		strategy: strategy,
		executor: concurrent.NewUnboundedExecutor(),
		storeState: storeState{
			blockManager:     newBlockManager(&cfg.blockManagerConfig),
			slotIndexManager: newSlotIndexManager(&cfg.slotIndexManagerConfig, strategy),
		},
	}
	var err error
	store.writer, err = store.newWriter(ctx)
	if err != nil {
		return nil, err
	}
	store.indexer, err = store.newIndexer(ctx)
	if err != nil {
		return nil, err
	}
	return store, nil
}

func (store *Store) Stop(ctx context.Context) error {
	store.executor.StopAndWait(ctx)
	return plz.CloseAll([]io.Closer{
		store.writer,
		store.indexer,
		store.slotIndexManager,
		store.blockManager,
	})
}

func (store *Store) Config() Config {
	return *store.cfg
}

func (store *Store) BatchWrite(ctxObj context.Context, resultChan chan<- WriteResult, entries []*Entry) {
	store.writer.BatchWrite(ctxObj, resultChan, entries)
}

func (store *Store) Write(ctxObj context.Context, entry *Entry) (Offset, error) {
	return store.writer.Write(ctxObj, entry)
}

func (store *Store) UpdateIndex(ctxObj context.Context) error {
	return store.indexer.UpdateIndex(ctxObj)
}

func (store *Store) RotateIndex(ctxObj context.Context) error {
	return store.indexer.RotateIndex(ctxObj)
}

// Remove can only remove those rows in indexed segment
// data still hanging in raw segments can not be removed
func (store *Store) Remove(ctxObj context.Context, untilOffset Offset) error {
	return store.indexer.Remove(ctxObj, untilOffset)
}
