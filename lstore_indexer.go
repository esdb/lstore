package lstore

import (
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"context"
	"github.com/esdb/gocodec"
)

type indexerCommand func(ctx countlog.Context)

type indexer struct {
	store          *Store
	commandQueue   chan indexerCommand
	currentVersion *StoreVersion
}

func (store *Store) newIndexer(ctx countlog.Context) *indexer {
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
	store.executor.Go(func(ctxObj context.Context) {
		ctx := countlog.Ctx(ctxObj)
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
				cmd = func(ctx countlog.Context) {
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
	indexer.asyncExecute(func(ctx countlog.Context) {
		resultChan <- indexer.doIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) edit() (*editingHead) {
	store := indexer.currentVersion
	blockManager := indexer.store.blockManager
	slotIndexManager := indexer.store.slotIndexManager
	strategy := indexer.store.IndexingStrategy
	editingHead := &editingHead{
		headSegmentVersion: store.headSegment.headSegmentVersion,
		strategy:           strategy,
		writeBlock:         blockManager.writeBlock,
		writeSlotIndex:     slotIndexManager.writeSlotIndex,
	}
	return editingHead
}

func (indexer *indexer) doIndex(ctx countlog.Context) (err error) {
	countlog.Trace("event!indexer.run")
	store := indexer.currentVersion
	if len(store.rawSegments) == 0 {
		return nil
	}
	editingHead := indexer.edit()
	purgedRawSegmentsCount := 0
	for _, rawSegment := range store.rawSegments {
		purgedRawSegmentsCount++
		// TODO: ensure rawSegment is actually blockLength
		blk := newBlock(rawSegment.startOffset, rawSegment.rows.rows)
		err := editingHead.addBlock(ctx, blk)
		ctx.TraceCall("callee!editingHead.addBlock", err)
		if err != nil {
			return err
		}
	}
	// ensure blocks are persisted
	headSegment := store.headSegment
	gocodec.UpdateChecksum(headSegment.writeMMap)
	err = headSegment.writeMMap.Flush()
	countlog.TraceCall("callee!headSegment.Flush", err,
		"tailBlockSeq", headSegment.tailBlockSeq,
		"tailSlotIndexSeq", headSegment.tailSlotIndexSeq,
		"tailOffset", headSegment.tailOffset)
	if err != nil {
		return err
	}
	err = indexer.store.writer.purgeRawSegments(ctx, purgedRawSegmentsCount)
	if err != nil {
		return err
	}
	return nil
}
