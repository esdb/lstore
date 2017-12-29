package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"github.com/esdb/biter"
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
	firstRow := store.rawSegments[0].rows[0]
	startSeq := firstRow.Seq
	compactingSegment := store.compactingSegment.nextSlot(startSeq, indexingStrategy, hashingStrategy)
	for _, rawSegment := range store.rawSegments {
		blk := newBlock(rawSegment.rows)
		newTailBlockSeq, blkHash, err := blockManager.writeBlock(compactingSegment.tailBlockSeq, blk)
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
		compactingSegment.tailBlockSeq = newTailBlockSeq
	}
	compactedRawSegmentsCount := len(store.rawSegments)
	newCompactingSegment, err := createCompactingSegment(
		compacter.store.CompactingSegmentTmpPath(), compactingSegment)
	if err != nil {
		countlog.Error("event!compacter.failed to create new compacting chunk", "err", err)
		compactionReq.Completed(err)
		return
	}
	compacter.switchCompactingSegment(newCompactingSegment, compactedRawSegmentsCount)
	compactionReq.Completed(nil)
	countlog.Info("event!compacter.compacting more chunk",
		"compactedRawSegmentsCount", compactedRawSegmentsCount)
}

func (compacter *compacter) switchCompactingSegment(
	newCompactingSegment *compactingSegment, compactedRawSegmentsCount int) {
	resultChan := make(chan error)
	compacter.store.asyncExecute(context.Background(), func(ctx context.Context, oldVersion *StoreVersion) {
		newVersion := oldVersion.edit()
		newVersion.rawSegments = oldVersion.rawSegments[compactedRawSegmentsCount:]
		newVersion.compactingSegment = newCompactingSegment
		compacter.store.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	<-resultChan
	return
}
