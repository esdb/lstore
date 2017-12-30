package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
)

type indexerCommand func(ctx context.Context)

type indexer struct {
	store          *Store
	commandQueue   chan indexerCommand
	currentVersion *StoreVersion
}

func (store *Store) newIndexer() *indexer {
	indexer := &indexer{
		store:          store,
		currentVersion: store.latest(),
		commandQueue:   make(chan indexerCommand, 1),
	}
	indexer.start()
	return indexer
}

func (indexer *indexer) start() {
	store := indexer.store
	indexer.currentVersion = store.latest()
	store.executor.Go(func(ctx context.Context) {
		defer func() {
			countlog.Info("event!indexer.stop")
			err := indexer.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		countlog.Info("event!indexer.start")
		for {
			timer := time.NewTimer(time.Second * 10)
			var cmd indexerCommand
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case cmd = <-indexer.commandQueue:
			}
			if store.isLatest(indexer.currentVersion) {
				if cmd == nil {
					return
				}
			} else {
				if err := indexer.currentVersion.Close(); err != nil {
					countlog.Error("event!indexer.failed to close version", "err", err)
				}
				indexer.currentVersion = store.latest()
			}
			if cmd == nil {
				cmd = func(ctx context.Context) {
					indexer.doIndex(ctx)
				}
			}
			cmd(ctx)
		}
	})
}

func (indexer *indexer) asyncExecute(cmd indexerCommand) error {
	select {
	case indexer.commandQueue <- cmd:
		return nil
	default:
		return errors.New("too many compaction request")
	}
}

func (indexer *indexer) Index() error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx context.Context) {
		resultChan <- indexer.doIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) doIndex(ctx context.Context) (err error) {
	// version will not change during compaction
	store := indexer.currentVersion
	blockManager := indexer.store.blockManager
	countlog.Trace("event!indexer.run")
	if len(store.rawSegments) == 0 {
		return nil
	}
	tailBlockSeq := store.indexedSegment.tailBlockSeq
	purgedRawSegmentsCount := 0
	for _, rawSegment := range store.rawSegments {
		purgedRawSegmentsCount++
		blk := newBlock(rawSegment.rows)
		tailBlockSeq, _, err = blockManager.writeBlock(tailBlockSeq, blk)
		if err != nil {
			return err
		}
	}
	err = indexer.store.writer.switchIndexedSegment(store.indexedSegment, purgedRawSegmentsCount)
	if err != nil {
		return err
	}
	return nil
}

func firstRowOf(rawSegments []*rawSegment) Row {
	return rawSegments[0].rows[0]
}
