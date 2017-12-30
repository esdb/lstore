package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"time"
	"errors"
	"github.com/esdb/biter"
	"os"
	"fmt"
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
	strategy := blockManager.indexingStrategy
	countlog.Trace("event!indexer.run")
	if len(store.rawSegments) == 0 {
		return nil
	}
	firstRow := firstRowOf(store.rawSegments)
	root, child := store.rootIndexedSegment.nextSlot(firstRow.Seq, strategy)
	countlog.Trace("event!indexer.next slot",
		"rootTailSlot", root.tailSlot, "childTailSlot", child.tailSlot)
	root.tailSeq = store.tailSegment.startSeq
	children := store.rootIndexedSegment.getChildren()
	for i, rawSegment := range store.rawSegments {
		if i != 0 {
			root, child = root.nextSlot(rawSegment.startSeq, strategy, child)
			countlog.Trace("event!indexer.next slot",
				"rootTailSlot", root.tailSlot, "childTailSlot", child.tailSlot)
		}
		blk := newBlock(rawSegment.rows)
		blockSeq := root.tailBlockSeq
		newTailBlockSeq, blkHash, err := blockManager.writeBlock(blockSeq, blk)
		if err != nil {
			countlog.Error("event!indexer.failed to write block",
				"tailBlockSeq", root.tailBlockSeq, "err", err)
			return err
		}
		childSlotMask := biter.SetBits[child.tailSlot]
		rootSlotMask := biter.SetBits[root.tailSlot]
		for i, childPbf := range child.slotIndex.pbfs {
			rootPbf := root.slotIndex.pbfs[i]
			for _, hashedElement := range blkHash[i] {
				childPbf.Put(childSlotMask, strategy.smallHashingStrategy.HashStage2(hashedElement))
				rootPbf.Put(rootSlotMask, strategy.bigHashingStrategy.HashStage2(hashedElement))
			}
		}
		child.children[child.tailSlot] = blockSeq
		root.tailBlockSeq = newTailBlockSeq
	}
	purgedRawSegmentsCount := len(store.rawSegments)
	childTmpPath := fmt.Sprintf("%s.%d", indexer.store.RootIndexedSegmentTmpPath(), root.tailSlot)
	childPath := fmt.Sprintf("%s.%d", indexer.store.RootIndexedSegmentPath(), root.tailSlot)
	childSegment, err := createIndexedSegment(childTmpPath, child)
	if err != nil {
		countlog.Error("event!indexer.failed to create indexed segment", "err", err)
		return err
	}
	err = os.Rename(childTmpPath, childPath)
	if err != nil {
		return err
	}
	children[root.tailSlot] = childSegment
	rootSegment, err := createRootIndexedSegment(indexer.store.RootIndexedSegmentTmpPath(), root, children)
	if err != nil {
		countlog.Error("event!indexer.failed to create root indexed segment", "err", err)
		return err
	}
	err = indexer.switchRootIndexedSegment(rootSegment, purgedRawSegmentsCount)
	if err != nil {
		countlog.Error("event!indexer.failed to switch root indexed segment",
			"err", err)
		return err
	}
	countlog.Info("event!indexer.indexed segments",
		"purgedRawSegmentsCount", purgedRawSegmentsCount)
	return nil
}

func (indexer *indexer) switchRootIndexedSegment(
	newRootIndexedSegment *rootIndexedSegment, purgedRawSegmentsCount int) error {
	resultChan := make(chan error)
	writer := indexer.store.writer
	writer.asyncExecute(context.Background(), func(ctx context.Context) {
		err := os.Rename(newRootIndexedSegment.path, indexer.store.RootIndexedSegmentPath())
		if err != nil {
			resultChan <- err
			return
		}
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		newVersion.rawSegments = oldVersion.rawSegments[purgedRawSegmentsCount:]
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
