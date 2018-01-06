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

func (indexer *indexer) UpdateIndex() error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx countlog.Context) {
		resultChan <- indexer.doIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) doIndex(ctx countlog.Context) (err error) {
	countlog.Debug("event!indexer.run")
	store := indexer.currentVersion
	if len(store.rawSegments) == 0 {
		countlog.Trace("event!indexingSegment.doIndex do not find enough raw segments",
			"rawSegmentsCount", len(store.rawSegments))
		return nil
	}
	indexing := store.indexingSegment
	originalTailOffset := indexing.tailOffset
	var blockRows []*Entry
	blockStartOffset := indexing.tailOffset
	var purgedRawSegmentsCount int
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
		blockRows = append(blockRows, rawSegment.rows[blockTailOffset - begin:]...)
		if len(blockRows) >= blockLength {
			purgedRawSegmentsCount = rawSegmentCount
			blk := newBlock(blockStartOffset, blockRows[:blockLength])
			err := indexing.addBlock(ctx, blk)
			ctx.TraceCall("callee!indexing.addBlock", err,
				"blockStartOffset", blockStartOffset,
					"purgedRawSegmentsCount", purgedRawSegmentsCount)
			if err != nil {
				return err
			}
			blockRows = blockRows[blockLength:]
			blockStartOffset += Offset(blockLength)
		}
	}
	if originalTailOffset == indexing.tailOffset {
		countlog.Trace("event!indexingSegment.doIndex can not find enough rows to build block",
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
	// TODO: purse raw segments
	err = indexer.store.writer.purgeRawSegments(ctx, purgedRawSegmentsCount)
	if err != nil {
		return err
	}
	return nil
}
