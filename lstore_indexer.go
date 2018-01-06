package lstore

import (
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"context"
	"github.com/esdb/gocodec"
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
				countlog.Trace("event!indexer.check is latest", "isLatest", true)
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
	for i, indexedSegment := range indexer.currentVersion.indexedSegments {
		if indexedSegment.startOffset >= untilOffset {
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
	prev := indexer.currentVersion.indexingSegment.searchable.indexSegment
	newIndexedSegment := &searchable{
		indexSegment:  prev.copy(),
		blockManager:  indexer.store.blockManager,
		readSlotIndex: indexer.store.slotIndexManager.readSlotIndex,
	}
	newIndexingSegment, err := openIndexingSegment(
		ctx, indexer.store.IndexingSegmentTmpPath(), prev,
		indexer.store.blockManager, indexer.store.slotIndexManager)
	if err != nil {
		return err
	}
	err = os.Rename(indexer.store.IndexingSegmentPath(),
		indexer.store.IndexedSegmentPath(newIndexedSegment.tailOffset))
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
	indexer.store.writer.rotateIndex(ctx, newIndexedSegment, newIndexingSegment)
	ctx.Info("event!indexer.rotated index",
		"indexedSegment.startOffset", newIndexedSegment.startOffset,
		"indexedSegment.tailOffset", newIndexedSegment.tailOffset)
	return nil
}

func (indexer *indexer) doUpdateIndex(ctx countlog.Context) (err error) {
	countlog.Debug("event!indexer.doUpdateIndex")
	store := indexer.currentVersion
	if len(store.rawSegments) == 0 {
		countlog.Trace("event!indexingSegment.doUpdateIndex do not find enough raw segments",
			"rawSegmentsCount", len(store.rawSegments))
		return nil
	}
	indexing := store.indexingSegment
	originalTailOffset := indexing.tailOffset
	var blockRows []*Entry
	blockStartOffset := indexing.tailOffset
	var removedRawSegmentsCount int
	for rawSegmentCount, rawSegment := range store.rawSegments {
		begin := rawSegment.startOffset
		end := begin + Offset(len(rawSegment.rows))
		blockTailOffset := blockStartOffset + Offset(len(blockRows))
		if blockTailOffset < begin {
			countlog.Fatal("event!indexer.offset is not continuous",
				"indexing.tailOffset", indexing.tailOffset,
				"blockStartOffset", blockStartOffset,
				"blockRowsCount", len(blockRows),
				"rawSegment.startOffset", begin)
			return errors.New("offset is not continuous")
		}
		if blockTailOffset >= end {
			continue
		}
		blockRows = append(blockRows, rawSegment.rows[blockTailOffset-begin:]...)
		if len(blockRows) >= blockLength {
			removedRawSegmentsCount = rawSegmentCount
			blk := newBlock(blockStartOffset, blockRows[:blockLength])
			err := indexing.addBlock(ctx, blk)
			ctx.TraceCall("callee!indexing.addBlock", err,
				"blockStartOffset", blockStartOffset,
				"removedRawSegmentsCount", removedRawSegmentsCount)
			if err != nil {
				return err
			}
			blockRows = blockRows[blockLength:]
			blockStartOffset += Offset(blockLength)
		}
	}
	if originalTailOffset == indexing.tailOffset {
		countlog.Trace("event!indexingSegment.doUpdateIndex can not find enough rows to build block",
			"originalTailOffset", originalTailOffset,
			"rawSegmentsCount", len(store.rawSegments),
			"blockRowsCount", len(blockRows))
		return nil
	}
	// ensure blocks are persisted
	gocodec.UpdateChecksum(indexing.writeMMap)
	err = indexing.writeMMap.Flush()
	countlog.TraceCall("callee!indexingSegment.Flush", err,
		"tailBlockSeq", indexing.tailBlockSeq,
		"tailSlotIndexSeq", indexing.tailSlotIndexSeq,
		"originalTailOffset", originalTailOffset,
		"tailOffset", indexing.tailOffset)
	if err != nil {
		return err
	}
	for level := level0; level <= indexing.topLevel; level++ {
		err = indexer.store.slotIndexManager.updateChecksum(indexing.levels[level], level)
		ctx.TraceCall("callee!slotIndexManager.updateChecksum", err)
		if err != nil {
			return err
		}
	}
	err = indexer.store.writer.removeRawSegments(ctx, removedRawSegmentsCount)
	if err != nil {
		return err
	}
	return nil
}
