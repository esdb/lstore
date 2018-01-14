package lstore

import (
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"context"
	"os"
	"github.com/v2pro/plz"
	"github.com/v2pro/plz/concurrent"
)

type indexerCommand func(ctx countlog.Context)

type indexer struct {
	cfg             *indexerConfig
	state           *storeState
	stateUpdater    storeStateUpdater
	commandQueue    chan indexerCommand
	currentVersion  *storeVersion
	slotIndexWriter slotIndexWriter
	blockWriter     blockWriter
}

func (store *Store) newIndexer(ctx countlog.Context) (*indexer, error) {
	indexer := &indexer{
		cfg:             &store.cfg.indexerConfig,
		state:           &store.storeState,
		stateUpdater:    store.writer,
		currentVersion:  store.latest(),
		commandQueue:    make(chan indexerCommand),
		slotIndexWriter: store.slotIndexManager.newWriter(14, 4),
		blockWriter:     store.blockManager.newWriter(),
	}
	err := indexer.load(ctx, store.slotIndexManager)
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
	state := indexer.state
	indexer.currentVersion = state.latest()
	executor.Go(func(ctxObj context.Context) {
		ctx := countlog.Ctx(ctxObj)
		defer func() {
			countlog.Info("event!indexer.stop")
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
			if cmd == nil {
				cmd = func(ctx countlog.Context) {
					indexer.doUpdateIndex(ctx)
				}
			}
			indexer.runCommand(ctx, cmd)
		}
	})
}

func (indexer *indexer) runCommand(ctx countlog.Context, cmd indexerCommand) {
	indexer.currentVersion = indexer.state.latest()
	indexer.state.lock(indexer, indexer.currentVersion.HeadOffset())
	defer indexer.state.unlock(indexer)
	cmd(ctx)
}

func (indexer *indexer) asyncExecute(ctx countlog.Context, cmd indexerCommand) error {
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
	indexer.asyncExecute(ctx, func(ctx countlog.Context) {
		resultChan <- indexer.doUpdateIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) RotateIndex(ctxObj context.Context) error {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan error)
	indexer.asyncExecute(ctx, func(ctx countlog.Context) {
		resultChan <- indexer.doRotateIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) Remove(ctxObj context.Context, untilOffset Offset) error {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan error)
	indexer.asyncExecute(ctx, func(ctx countlog.Context) {
		resultChan <- indexer.doRemove(ctx, untilOffset)
	})
	return <-resultChan
}

func (indexer *indexer) doRemove(ctx countlog.Context, untilOffset Offset) (err error) {
	var removedIndexedSegmentsCount int
	var tailBlockSeq blockSeq
	var tailSlotIndexSeq slotIndexSeq
	for i, indexedSegment := range indexer.currentVersion.indexedSegments {
		if indexedSegment.headOffset >= untilOffset {
			break
		}
		if indexedSegment.tailOffset <= untilOffset {
			removedIndexedSegmentsCount = i + 1
			err = os.Remove(indexer.cfg.IndexedSegmentPath(indexedSegment.tailOffset))
			ctx.TraceCall("callee!os.Remove", err)
			if err != nil {
				return err
			}
			tailBlockSeq = indexedSegment.tailBlockSeq
			tailSlotIndexSeq = indexedSegment.tailSlotIndexSeq
		}
	}
	if removedIndexedSegmentsCount == 0 {
		return nil
	}
	// TODO: calculate indexedSegments
	err = indexer.stateUpdater.removedIndex(ctx, nil)
	if err != nil {
		return err
	}
	indexer.slotIndexWriter.remove(tailSlotIndexSeq)
	indexer.blockWriter.remove(tailBlockSeq)
	return nil
}

func (indexer *indexer) doRotateIndex(ctx countlog.Context) (err error) {
	countlog.Debug("event!indexer.doRotateIndex")
	currentVersion := indexer.currentVersion
	oldIndexingSegment := currentVersion.indexingSegment
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
	indexer.stateUpdater.rotatedIndex(ctx, currentVersion.indexingSegment, newIndexingSegment)
	ctx.Info("event!indexer.rotated index",
		"indexedSegment.headOffset", oldIndexingSegment.headOffset,
		"indexedSegment.tailOffset", oldIndexingSegment.tailOffset)
	return nil
}

func (indexer *indexer) doUpdateIndex(ctx countlog.Context) (err error) {
	currentVersion := indexer.currentVersion
	storeTailOffset := indexer.state.getTailOffset()
	oldIndexingSegment := currentVersion.indexingSegment
	oldIndexingTailOffset := oldIndexingSegment.tailOffset
	countlog.Debug("event!indexer.doUpdateIndex",
		"storeTailOffset", storeTailOffset,
		"oldIndexingTailOffset", oldIndexingTailOffset)
	if int(storeTailOffset-oldIndexingTailOffset) < blockLength {
		countlog.Debug("event!indexer.doUpdateIndex do not find enough raw entries")
		return nil
	}
	firstRawChunk := currentVersion.chunks[0]
	if firstRawChunk.headOffset+Offset(firstRawChunk.headSlot<<6) != oldIndexingTailOffset {
		countlog.Fatal("event!indexer.doUpdateIndex find offset inconsistent")
		return errors.New("inconsistent tail offset")
	}
	if firstRawChunk.tailSlot < firstRawChunk.headSlot+3 {
		countlog.Fatal("event!indexer.doUpdateIndex find firstRawChunk not fully filled",
			"tailSlot", firstRawChunk.tailSlot,
			"headSlot", firstRawChunk.headSlot)
		return errors.New("firstRawChunk not fully filled")
	}
	var blockRows []*Entry
	for _, rawChunkChild := range firstRawChunk.children[firstRawChunk.headSlot:firstRawChunk.headSlot+4] {
		blockRows = append(blockRows, rawChunkChild.children...)
	}
	blk := newBlock(oldIndexingTailOffset, blockRows[:blockLength])
	indexingSegment := oldIndexingSegment.copy()
	if err != nil {
		return err
	}
	err = indexingSegment.addBlock(ctx, indexer.slotIndexWriter, indexer.blockWriter, blk)
	ctx.TraceCall("callee!indexingSegment.addBlock", err,
		"blockStartOffset", oldIndexingTailOffset,
		"indexingSegmentTailOffset", indexingSegment.tailOffset,
		"tailBlockSeq", indexingSegment.tailBlockSeq,
		"tailSlotIndexSeq", indexingSegment.tailSlotIndexSeq)
	if err != nil {
		return err
	}
	// TODO: rotate
	err = indexer.saveIndexingSegment(ctx, indexingSegment, false)
	ctx.TraceCall("callee!indexingSegment.save", err)
	if err != nil {
		return err
	}
	err = indexer.stateUpdater.movedBlockIntoIndex(ctx, indexingSegment)
	if err != nil {
		return err
	}
	return nil
}

func (indexer *indexer) saveIndexingSegment(ctx countlog.Context, indexingSegment *indexSegment, shouldRotate bool) error {
	err := createIndexSegment(ctx, indexer.cfg.IndexingSegmentTmpPath(), indexingSegment)
	if err != nil {
		return err
	}
	if shouldRotate {
		err = os.Rename(indexer.cfg.IndexingSegmentPath(), indexer.cfg.IndexedSegmentPath(indexingSegment.headOffset))
		ctx.TraceCall("callee!os.Rename", err)
		if err != nil {
			return err
		}
	}
	err = os.Rename(indexer.cfg.IndexingSegmentTmpPath(), indexer.cfg.IndexingSegmentPath())
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	return nil
}
