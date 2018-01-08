package lstore

import (
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"context"
	"os"
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
				countlog.Trace("event!indexer.check is latest", "isLatest", true)
				if cmd == nil {
					return
				}
			} else {
				indexer.currentVersion = store.latest()
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
	slotIndexManager := indexer.store.slotIndexManager
	slotIndexManager.dataManager.Lock(indexer)
	defer slotIndexManager.dataManager.Unlock(indexer)
	blockManager := indexer.store.blockManager
	blockManager.dataManager.Lock(indexer)
	defer blockManager.dataManager.Unlock(indexer)
	cmd(ctx)
}

func (indexer *indexer) asyncExecute(cmd indexerCommand) error {
	select {
	case indexer.commandQueue <- cmd:
		return nil
	default:
		return errors.New("too many compaction request")
	}
}

func (indexer *indexer) UpdateIndex() error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx countlog.Context) {
		resultChan <- indexer.doUpdateIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) RotateIndex() error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx countlog.Context) {
		resultChan <- indexer.doRotateIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) Remove(untilOffset Offset) error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx countlog.Context) {
		resultChan <- indexer.doRemove(ctx, untilOffset)
	})
	return <-resultChan
}

func (indexer *indexer) doRemove(ctx countlog.Context, untilOffset Offset) (err error) {
	var removedIndexedSegmentsCount int
	var tailBlockSeq blockSeq
	var tailSlotIndexSeq slotIndexSeq
	for i, indexedSegment := range indexer.currentVersion.indexedChunks {
		if indexedSegment.headOffset >= untilOffset {
			break
		}
		if indexedSegment.tailOffset <= untilOffset {
			removedIndexedSegmentsCount = i + 1
			err = os.Remove(indexer.store.IndexedSegmentPath(indexedSegment.tailOffset))
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
	err = indexer.store.writer.removedIndexedSegments(ctx, removedIndexedSegmentsCount)
	if err != nil {
		return err
	}
	indexer.store.slotIndexManager.remove(tailSlotIndexSeq)
	indexer.store.blockManager.remove(tailBlockSeq)
	return nil
}

func (indexer *indexer) doRotateIndex(ctx countlog.Context) (err error) {
	countlog.Debug("event!indexer.doRotateIndex")
	currentVersion := indexer.currentVersion
	oldIndexingSegment := currentVersion.indexingChunk.indexSegment
	newIndexingSegment, err := newIndexSegment(indexer.store.slotIndexManager, oldIndexingSegment)
	if err != nil {
		return err
	}
	err = createIndexSegment(ctx, indexer.store.IndexingSegmentTmpPath(), newIndexingSegment)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.store.IndexingSegmentPath(),
		indexer.store.IndexedSegmentPath(newIndexingSegment.headOffset))
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.store.IndexingSegmentTmpPath(),
		indexer.store.IndexingSegmentPath())
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	indexer.store.writer.rotateIndex(ctx, currentVersion.indexingChunk, &indexChunk{
		indexSegment: newIndexingSegment,
		blockManager: indexer.store.blockManager,
		readSlotIndex: indexer.store.slotIndexManager.mapWritableSlotIndex,
	})
	ctx.Info("event!indexer.rotated index",
		"indexedSegment.headOffset", oldIndexingSegment.headOffset,
		"indexedSegment.tailOffset", oldIndexingSegment.tailOffset)
	return nil
}

func (indexer *indexer) doUpdateIndex(ctx countlog.Context) (err error) {
	countlog.Debug("event!indexer.doUpdateIndex")
	currentVersion := indexer.currentVersion
	storeTailOffset := indexer.store.getTailOffset()
	indexingTailOffset := currentVersion.indexingChunk.tailOffset
	if int(storeTailOffset-indexingTailOffset) < blockLength {
		countlog.Debug("event!indexingChunk.doUpdateIndex do not find enough raw entries",
			"storeTailOffset", storeTailOffset,
				"indexingTailOffset", indexingTailOffset)
		return nil
	}
	if currentVersion.rawChunks[0].headOffset != indexingTailOffset {
		countlog.Fatal("event!indexingChunk.doUpdateIndex find offset inconsistent",
			"storeTailOffset", storeTailOffset,
			"indexingTailOffset", indexingTailOffset)
		return errors.New("inconsistent tail offset")
	}
	var blockRows []*Entry
	for _, rawChunkChild := range currentVersion.rawChunks[0].children[:4] {
		blockRows = append(blockRows, rawChunkChild.children...)
	}
	blk := newBlock(indexingTailOffset, blockRows[:blockLength])
	indexingChunk, err := indexer.newIndexingChunk()
	if err != nil {
		return err
	}
	err = indexingChunk.addBlock(ctx, blk)
	ctx.TraceCall("callee!indexingChunk.addBlock", err,
		"blockStartOffset", indexingTailOffset)
	if err != nil {
		return err
	}
	err = indexer.saveIndexingChunk(ctx, indexingChunk.indexSegment)
	ctx.TraceCall("callee!indexingChunk.save", err)
	if err != nil {
		return err
	}
	err = indexer.store.writer.updateIndex(ctx, &indexChunk{
		indexSegment: indexingChunk.indexSegment,
		blockManager: indexer.store.blockManager,
		readSlotIndex: indexer.store.slotIndexManager.mapWritableSlotIndex,
	})
	if err != nil {
		return err
	}
	return nil
}

func (indexer *indexer) newIndexingChunk() (*indexingChunk, error) {
	currentVersion := indexer.currentVersion
	slotIndexManager := indexer.store.slotIndexManager
	indexingSegment, err := newIndexSegment(slotIndexManager, currentVersion.indexingChunk.indexSegment)
	if err != nil {
		return nil, err
	}
	indexingChunk := &indexingChunk{
		indexSegment:     indexingSegment,
		indexingLevels:   make([]*slotIndex, levelsCount),
		blockManager:     indexer.store.blockManager,
		slotIndexManager: slotIndexManager,
		strategy:         indexer.store.IndexingStrategy,
	}
	for i := level0; i <= indexingSegment.topLevel; i++ {
		indexingChunk.indexingLevels[i], err = slotIndexManager.mapWritableSlotIndex(indexingChunk.levels[i], i)
		if err != nil {
			return nil, err
		}
	}
	return indexingChunk, nil
}


func (indexer *indexer) saveIndexingChunk(ctx countlog.Context, indexingSegment *indexSegment) error {
	err := createIndexSegment(ctx, indexer.store.IndexingSegmentTmpPath(), indexingSegment)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.store.IndexingSegmentPath(), indexer.store.IndexedSegmentPath(indexingSegment.headOffset))
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.store.IndexingSegmentTmpPath(), indexer.store.IndexingSegmentPath())
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	for level := level0; level <= indexingSegment.topLevel; level++ {
		err = indexer.store.slotIndexManager.updateChecksum(indexingSegment.levels[level], level)
		ctx.TraceCall("callee!slotIndexManager.updateChecksum", err)
		if err != nil {
			return err
		}
	}
	return nil
}