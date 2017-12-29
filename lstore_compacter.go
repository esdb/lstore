package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"github.com/esdb/biter"
	"os"
)

type compactionRequest chan error

func (req compactionRequest) Completed(err error) {
	req <- err
}

type compacter struct {
	store                 *Store
	compactionRequestChan chan compactionRequest
	currentVersion        *StoreVersion
}

func (store *Store) newCompacter() *compacter {
	compacter := &compacter{
		store:                 store,
		currentVersion:        store.latest(),
		compactionRequestChan: make(chan compactionRequest, 1),
	}
	compacter.start()
	return compacter
}

func (compacter *compacter) Compact() error {
	request := make(compactionRequest)
	select {
	case compacter.compactionRequestChan <- request:
	default:
		return errors.New("too many compaction request")
	}
	return <-request
}

func (compacter *compacter) start() {
	store := compacter.store
	compacter.currentVersion = store.latest()
	store.executor.Go(func(ctx context.Context) {
		defer func() {
			countlog.Info("event!compacter.stop")
			err := compacter.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		countlog.Info("event!compacter.start")
		if store.CompactAfterStartup {
			compacter.compact(nil)
		}
		for {
			timer := time.NewTimer(time.Second * 10)
			var compactionReq compactionRequest
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case compactionReq = <-compacter.compactionRequestChan:
			}
			if store.isLatest(compacter.currentVersion) {
				// nothing to compact
				continue
			}
			if err := compacter.currentVersion.Close(); err != nil {
				countlog.Error("event!compacter.failed to close version", "err", err)
			}
			compacter.currentVersion = store.latest()
			compacter.compact(compactionReq)
		}
	})
}

func (compacter *compacter) compact(compactionReq compactionRequest) {
	// version will not change during compaction
	store := compacter.currentVersion
	blockManager := compacter.store.blockManager
	indexingStrategy := blockManager.indexingStrategy
	hashingStrategy := compacter.store.hashingStrategy
	countlog.Trace("event!compacter.run")
	if len(store.rawSegments) == 0 {
		return
	}
	firstRow := firstRowOf(store.rawSegments)
	compactingSegment := store.compactingSegment.nextSlot(firstRow.Seq, indexingStrategy, hashingStrategy)
	compactingSegment.tailSeq = store.tailSegment.StartSeq
	for _, rawSegment := range store.rawSegments {
		blk := newBlock(rawSegment.rows)
		blockSeq := compactingSegment.tailBlockSeq
		newTailBlockSeq, blkHash, err := blockManager.writeBlock(blockSeq, blk)
		if err != nil {
			countlog.Error("event!compacter.failed to write block",
				"tailBlockSeq", compactingSegment.tailBlockSeq, "err", err)
			compactionReq.Completed(err)
			return
		}
		for i, pbf := range compactingSegment.slotIndex.pbfs {
			for _, hashedElement := range blkHash[i] {
				pbf.Put(biter.SetBits[compactingSegment.tailSlot], hashingStrategy.HashStage2(hashedElement))
			}
		}
		compactingSegment.blocks[compactingSegment.tailSlot] = blockSeq
		compactingSegment.tailBlockSeq = newTailBlockSeq
		compactingSegment.tailSlot++
	}
	compactedRawSegmentsCount := len(store.rawSegments)
	newCompactingSegment, err := createCompactingSegment(
		compacter.store.CompactingSegmentTmpPath(), compactingSegment)
	if err != nil {
		countlog.Error("event!compacter.failed to create new compacting chunk", "err", err)
		compactionReq.Completed(err)
		return
	}
	err = compacter.switchCompactingSegment(newCompactingSegment, compactedRawSegmentsCount)
	if err != nil {
		countlog.Error("event!compacter.failed to switch compacting segment",
			"err", err)
		compactionReq.Completed(err)
		return
	}
	compactionReq.Completed(nil)
	countlog.Info("event!compacter.compacting more chunk",
		"compactedRawSegmentsCount", compactedRawSegmentsCount)
}

func (compacter *compacter) switchCompactingSegment(
	newCompactingSegment *compactingSegment, compactedRawSegmentsCount int) error {
	resultChan := make(chan error)
	compacter.store.asyncExecute(context.Background(), func(ctx context.Context, oldVersion *StoreVersion) {
		err := os.Rename(newCompactingSegment.path, compacter.store.CompactingSegmentPath())
		if err != nil {
			resultChan <- err
			return
		}
		newVersion := oldVersion.edit()
		newVersion.rawSegments = oldVersion.rawSegments[compactedRawSegmentsCount:]
		newVersion.compactingSegment = newCompactingSegment
		compacter.store.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	return <-resultChan
}

func firstRowOf(rawSegments []*RawSegment) Row {
	return rawSegments[0].rows[0]
}
