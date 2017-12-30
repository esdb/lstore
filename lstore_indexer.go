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

func (store *Store) newIndexer() *indexer {
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
				cmd = func(ctx context.Context) {
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
	indexer.asyncExecute(func(ctx context.Context) {
		resultChan <- indexer.doIndex(ctx)
	})
	return <-resultChan
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
	root, child := store.rootIndexedSegment.currentSlot(firstRow.Seq, strategy)
	countlog.Trace("event!indexer.current slot",
		"rootTailSlot", root.tailSlot, "childTailSlot", child.tailSlot)
	children := store.rootIndexedSegment.getChildren()
	var purgedRawSegmentsCount int
	for purgedRawSegmentsCount := 0; purgedRawSegmentsCount < len(store.rawSegments); purgedRawSegmentsCount++ {
		rawSegment := store.rawSegments[purgedRawSegmentsCount]
		if child.isFull() {
			child.tailSeq = rawSegment.startSeq
			err := indexer.writeIndexedSegment(root.tailSlot, child, children)
			if err != nil {
				return err
			}
			if root.isFull() {
				root.tailSeq = rawSegment.startSeq
				err := indexer.writeRootIndexedSegment(root, children, purgedRawSegmentsCount)
				if err != nil {
					return err
				}
				return nil
			}
		}
		root, child = root.nextSlot(rawSegment.startSeq, strategy, child)
		countlog.Trace("event!indexer.next slot",
			"rootTailSlot", root.tailSlot, "childTailSlot", child.tailSlot)
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
	root.tailSeq = store.tailSegment.startSeq
	child.tailSeq = store.tailSegment.startSeq
	err := indexer.writeIndexedSegment(root.tailSlot, child, children)
	if err != nil {
		return err
	}
	err = indexer.writeRootIndexedSegment(root, children, purgedRawSegmentsCount)
	if err != nil {
		return err
	}
	return nil
}

func (indexer *indexer) writeIndexedSegment(
	slot biter.Slot, version indexedSegmentVersion, children []*indexedSegment) error {
	childTmpPath := fmt.Sprintf("%s.%d", indexer.store.RootIndexedSegmentTmpPath(), slot)
	childPath := fmt.Sprintf("%s.%d", indexer.store.RootIndexedSegmentPath(), slot)
	childSegment, err := createIndexedSegment(childTmpPath, version)
	if err != nil {
		countlog.Error("event!indexer.failed to create indexed segment", "err", err)
		return err
	}
	err = os.Rename(childTmpPath, childPath)
	if err != nil {
		return err
	}
	countlog.Trace("event!indexer.renamed indexedSegment", "from", childTmpPath, "to", childPath)
	children[slot] = childSegment
	return nil
}

func (indexer *indexer) writeRootIndexedSegment(
	root rootIndexedSegmentVersion, children []*indexedSegment, purgedRawSegmentsCount int) error {
	rootSegmentPath := indexer.store.RootIndexedSegmentTmpPath()
	rootSegment, err := createRootIndexedSegment(rootSegmentPath, root, children)
	if err != nil {
		countlog.Error("event!indexer.failed to create root indexed segment", "err", err)
		return err
	}
	err = os.Rename(rootSegmentPath, indexer.store.RootIndexedSegmentPath())
	if err != nil {
		countlog.Error("event!indexer.failed to rename rootIndexedSegment file",
			"from", rootSegmentPath,
			"to", indexer.store.RootIndexedSegmentPath(),
			"err", err)
		return err
	}
	countlog.Trace("event!indexer.renamed rootIndexedSegment file",
		"from", rootSegmentPath,
		"to", indexer.store.RootIndexedSegmentPath())
	for _, rawSegment := range indexer.currentVersion.rawSegments[:purgedRawSegmentsCount] {
		err := os.Remove(rawSegment.Path)
		if err != nil {
			return err
		}
	}
	err = indexer.store.writer.switchRootIndexedSegment(rootSegment, purgedRawSegmentsCount)
	if err != nil {
		countlog.Error("event!indexer.failed to switch root indexed segment",
			"err", err)
		return err
	}
	countlog.Info("event!indexer.indexed segments",
		"purgedRawSegmentsCount", purgedRawSegmentsCount)
	return nil
}

func firstRowOf(rawSegments []*rawSegment) Row {
	return rawSegments[0].rows[0]
}
