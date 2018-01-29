package lstore

import (
	"context"
	"errors"
	"github.com/v2pro/plz"
	"github.com/v2pro/plz/concurrent"
	"github.com/v2pro/plz/countlog"
	"os"
	"time"
)

type indexerCommand func(ctx *countlog.Context)

type indexer struct {
	cfg             *indexerConfig
	state           *storeState
	appender        *appender
	commandQueue    chan indexerCommand
	slotIndexWriter slotIndexWriter
	blockWriter     blockWriter
	indexingSegment *indexSegment
}

func (store *Store) newIndexer(ctx *countlog.Context) (*indexer, error) {
	cfg := store.cfg
	if cfg.UpdateIndexInterval == 0 {
		cfg.UpdateIndexInterval = time.Millisecond * 100
	}
	indexer := &indexer{
		cfg:             &cfg.indexerConfig,
		state:           &store.storeState,
		appender:        store.appender,
		commandQueue:    make(chan indexerCommand),
		slotIndexWriter: store.slotIndexManager.newWriter(14, 4),
		blockWriter:     store.blockManager.newWriter(),
	}
	err := indexer.loadIndex(ctx, store.slotIndexManager)
	if err != nil {
		return nil, err
	}
	indexer.start(store.executor)
	return indexer, nil
}

func (indexer *indexer) Close() error {
	return plz.Close(indexer.slotIndexWriter)
}

func (indexer *indexer) start(executor *concurrent.UnboundedExecutor) {
	executor.Go(func(ctx *countlog.Context) {
		defer func() {
			countlog.Info("event!indexer.stop")
		}()
		countlog.Info("event!indexer.start")
		for {
			timer := time.NewTimer(indexer.cfg.UpdateIndexInterval)
			var cmd indexerCommand
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case cmd = <-indexer.commandQueue:
			}
			if cmd == nil {
				cmd = func(ctx *countlog.Context) {
					indexer.doUpdateIndex(ctx)
				}
			}
			indexer.runCommand(ctx, cmd)
		}
	})
}

func (indexer *indexer) runCommand(ctx *countlog.Context, cmd indexerCommand) {
	defer func() {
		recovered := recover()
		if recovered == concurrent.StopSignal {
			panic(concurrent.StopSignal)
		}
		countlog.LogPanic(recovered)
	}()
	indexer.slotIndexWriter.gc()
	cmd(ctx)
}

func (indexer *indexer) asyncExecute(ctx *countlog.Context, cmd indexerCommand) error {
	select {
	case indexer.commandQueue <- cmd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (indexer *indexer) UpdateIndex(ctxObj context.Context) error {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan error)
	indexer.asyncExecute(ctx, func(ctx *countlog.Context) {
		resultChan <- indexer.doUpdateIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) RotateIndex(ctxObj context.Context) error {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan error)
	indexer.asyncExecute(ctx, func(ctx *countlog.Context) {
		resultChan <- indexer.doRotateIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) doRotateIndex(ctx *countlog.Context) (err error) {
	countlog.Debug("event!indexer.doRotateIndex")
	oldIndexingSegment := indexer.indexingSegment
	newIndexingSegment, err := newIndexSegment(indexer.slotIndexWriter, oldIndexingSegment)
	if err != nil {
		return err
	}
	err = createIndexSegment(ctx, indexer.cfg.IndexingSegmentTmpPath(), newIndexingSegment)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.cfg.IndexingSegmentPath(),
		indexer.cfg.IndexedSegmentPath(newIndexingSegment.headOffset))
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.cfg.IndexingSegmentTmpPath(),
		indexer.cfg.IndexingSegmentPath())
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	indexer.indexingSegment = newIndexingSegment
	indexer.state.rotatedIndex(oldIndexingSegment, newIndexingSegment)
	ctx.Info("event!indexer.rotated index",
		"indexedSegment.headOffset", oldIndexingSegment.headOffset,
		"indexedSegment.tailOffset", oldIndexingSegment.tailOffset)
	return nil
}

func (indexer *indexer) doUpdateIndex(ctx *countlog.Context) (err error) {
	updated := true
	for updated {
		updated, err = indexer.updateOnce(ctx)
		if err != nil {
			return err
		}
		if indexer.cfg.IndexSegmentMaxEntriesCount == 0 {
			continue
		}
		indexingSegment := indexer.indexingSegment
		if int(indexingSegment.tailOffset-indexingSegment.headOffset) > indexer.cfg.IndexSegmentMaxEntriesCount {
			err = indexer.doRotateIndex(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (indexer *indexer) updateOnce(ctx *countlog.Context) (updated bool, err error) {
	currentVersion := indexer.state.latest()
	oldIndexingSegment := currentVersion.indexingSegment
	oldIndexingTailOffset := oldIndexingSegment.tailOffset
	countlog.Debug("event!indexer.doUpdateIndex",
		"oldIndexingTailOffset", oldIndexingTailOffset)
	chunks := currentVersion.appendedChunks
	if len(chunks) == 0 {
		countlog.Debug("event!indexer.doUpdateIndex do not find enough raw entries")
		return false, nil
	}
	indexingSegment := oldIndexingSegment.copy()
	if err != nil {
		return false, err
	}
	blockHeadOffset := oldIndexingTailOffset
	blockEntries := make([]*Entry, 0, blockLength)
	for _, chunk := range chunks {
		for _, chunkChild := range chunk.children[:chunk.tailSlot] {
			blockEntries = append(blockEntries, chunkChild.children...)
			if len(blockEntries) == blockLength {
				blk := newBlock(blockHeadOffset, blockEntries)
				err = indexingSegment.addBlock(ctx, indexer.slotIndexWriter, indexer.blockWriter, blk)
				ctx.TraceCall("callee!indexingSegment.addBlock", err,
					"blockHeadOffset", blockHeadOffset,
					"indexingSegmentTailOffset", indexingSegment.tailOffset,
					"tailBlockSeq", indexingSegment.tailBlockSeq,
					"tailSlotIndexSeq", indexingSegment.tailSlotIndexSeq)
				if err != nil {
					return false, err
				}
				blockHeadOffset += Offset(blockLength)
				blockEntries = blockEntries[:0]
			}
		}
	}
	if len(blockEntries) != 0 {
		countlog.Fatal("event!indexer.appended chunks must have entries count multiplier of 256",
			"chunksCount", len(chunks))
		return false, errors.New("appended chunks must have entries count multiplier of 256")
	}
	err = indexer.saveIndexingSegment(ctx, indexingSegment)
	ctx.TraceCall("callee!indexingSegment.save", err)
	if err != nil {
		return false, err
	}
	indexer.indexingSegment = indexingSegment
	indexer.state.movedChunksIntoIndex(indexingSegment, len(chunks))
	indexer.appender.removeRawSegments(ctx, indexingSegment.tailOffset)
	return true, nil
}

func (indexer *indexer) saveIndexingSegment(ctx *countlog.Context, indexingSegment *indexSegment) error {
	err := createIndexSegment(ctx, indexer.cfg.IndexingSegmentTmpPath(), indexingSegment)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.cfg.IndexingSegmentTmpPath(), indexer.cfg.IndexingSegmentPath())
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	return nil
}

func (indexer *indexer) removeIndexedSegments(ctx *countlog.Context, removedSegments []*indexSegment) {
	for _, removedSegment := range removedSegments {
		segmentPath := indexer.cfg.IndexedSegmentPath(removedSegment.headOffset)
		err := os.Remove(segmentPath)
		ctx.TraceCall("callee!os.Remove", err)
	}
	lastRemovedSegment := removedSegments[len(removedSegments)-1]
	indexer.slotIndexWriter.remove(lastRemovedSegment.tailSlotIndexSeq)
	indexer.blockWriter.remove(lastRemovedSegment.tailBlockSeq)
}
