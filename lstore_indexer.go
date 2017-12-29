package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"github.com/esdb/biter"
	"os"
)

type indexerCommand func(ctx context.Context)

type indexer struct {
	store          *Store
	commandQueue   chan indexerCommand
	currentVersion *StoreVersion
}

func (store *Store) newCompacter() *indexer {
	compacter := &indexer{
		store:          store,
		currentVersion: store.latest(),
		commandQueue:   make(chan indexerCommand, 1),
	}
	compacter.start()
	return compacter
}

func (indexer *indexer) Index() error {
	resultChan := make(chan error)
	indexer.asyncExecute(func(ctx context.Context) {
		resultChan <- indexer.doIndex(ctx)
	})
	return <-resultChan
}

func (indexer *indexer) asyncExecute(cmd indexerCommand) error {
	select {
	case indexer.commandQueue <- cmd:
		return nil
	default:
		return errors.New("too many compaction request")
	}
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
				// nothing to compact
				continue
			}
			if err := indexer.currentVersion.Close(); err != nil {
				countlog.Error("event!indexer.failed to close version", "err", err)
			}
			indexer.currentVersion = store.latest()
			cmd(ctx)
		}
	})
}

func (indexer *indexer) doIndex(ctx context.Context) error {
	// version will not change during compaction
	store := indexer.currentVersion
	blockManager := indexer.store.blockManager
	indexingStrategy := blockManager.indexingStrategy
	hashingStrategy := indexer.store.hashingStrategy
	countlog.Trace("event!indexer.run")
	if len(store.rawSegments) == 0 {
		return
	}
	firstRow := firstRowOf(store.rawSegments)
	compactingSegment := store.rootIndexedSegment.nextSlot(firstRow.Seq, indexingStrategy, hashingStrategy)
	compactingSegment.tailSeq = store.tailSegment.startSeq
	for _, rawSegment := range store.rawSegments {
		blk := newBlock(rawSegment.rows)
		blockSeq := compactingSegment.tailBlockSeq
		newTailBlockSeq, blkHash, err := blockManager.writeBlock(blockSeq, blk)
		if err != nil {
			countlog.Error("event!indexer.failed to write block",
				"tailBlockSeq", compactingSegment.tailBlockSeq, "err", err)
			return err
		}
		for i, pbf := range compactingSegment.slotIndex.pbfs {
			for _, hashedElement := range blkHash[i] {
				pbf.Put(biter.SetBits[compactingSegment.tailSlot], hashingStrategy.HashStage2(hashedElement))
			}
		}
		compactingSegment.children[compactingSegment.tailSlot] = blockSeq
		compactingSegment.tailBlockSeq = newTailBlockSeq
		compactingSegment.tailSlot++
	}
	compactedRawSegmentsCount := len(store.rawSegments)
	newCompactingSegment, err := createIndexedSegment(
		indexer.store.CompactingSegmentTmpPath(), compactingSegment)
	if err != nil {
		countlog.Error("event!indexer.failed to create new compacting chunk", "err", err)
		return err
	}
	err = indexer.switchCompactingSegment(newCompactingSegment, compactedRawSegmentsCount)
	if err != nil {
		countlog.Error("event!indexer.failed to switch compacting segment",
			"err", err)
		return err
	}
	countlog.Info("event!indexer.compacting more chunk",
		"compactedRawSegmentsCount", compactedRawSegmentsCount)
	return nil
}

func (indexer *indexer) switchCompactingSegment(
	newRootIndexedSegment *rootIndexedSegment, compactedRawSegmentsCount int) error {
	resultChan := make(chan error)
	writer := indexer.store.writer
	writer.asyncExecute(context.Background(), func(ctx context.Context) {
		err := os.Rename(newRootIndexedSegment.path, indexer.store.CompactingSegmentPath())
		if err != nil {
			resultChan <- err
			return
		}
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		newVersion.rawSegments = oldVersion.rawSegments[compactedRawSegmentsCount:]
		newVersion.rootIndexedSegment = newRootIndexedSegment
		indexer.store.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	return <-resultChan
}

func firstRowOf(rawSegments []*RawSegment) Row {
	return rawSegments[0].rows[0]
}
