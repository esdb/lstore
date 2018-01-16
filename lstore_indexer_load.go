package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"os"
)

func (indexer *indexer) load(ctx countlog.Context, slotIndexManager slotIndexManager) error {
	indexingSegment, err := openIndexSegment(
		ctx, indexer.cfg.IndexingSegmentPath())
	if os.IsNotExist(err) {
		slotIndexWriter := slotIndexManager.newWriter(10, 4)
		defer plz.Close(slotIndexWriter)
		indexingSegment, err = newIndexSegment(slotIndexWriter, nil)
		if err != nil {
			return err
		}
		err = createIndexSegment(ctx, indexer.cfg.IndexingSegmentPath(), indexingSegment)
		if err != nil {
			return err
		}
	} else if err != nil {
		ctx.TraceCall("callee!store.openIndexingSegment", err)
		return err
	}
	startOffset := indexingSegment.headOffset
	var reversedIndexedSegments []*indexSegment
	for {
		indexedSegmentPath := indexer.cfg.IndexedSegmentPath(startOffset)
		indexedSegment, err := openIndexSegment(ctx, indexedSegmentPath)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		reversedIndexedSegments = append(reversedIndexedSegments, indexedSegment)
		startOffset = indexedSegment.headOffset
	}
	indexedSegments := make([]*indexSegment, len(reversedIndexedSegments))
	for i := 0; i < len(reversedIndexedSegments); i++ {
		indexedSegments[i] = reversedIndexedSegments[len(reversedIndexedSegments)-i-1]
	}
	slotIndexReader := slotIndexManager.newReader(14, 4)
	defer slotIndexReader.Close()
	level0Index, err := slotIndexReader.readSlotIndex(indexingSegment.levels[0], 0)
	if err != nil {
		return err
	}
	level1Index, err := slotIndexReader.readSlotIndex(indexingSegment.levels[1], 1)
	if err != nil {
		return err
	}
	level2Index, err := slotIndexReader.readSlotIndex(indexingSegment.levels[2], 2)
	if err != nil {
		return err
	}
	err = indexer.appender.loadedIndex(ctx, indexedSegments, indexingSegment)
	if err != nil {
		return err
	}
	indexer.indexingSegment = indexingSegment
	ctx.Info("event!indexer.load",
		"indexedSegmentsCount", len(indexedSegments),
		"indexingHeadOffset", indexingSegment.headOffset,
		"indexingTailOffset", indexingSegment.tailOffset,
		"indexingTopLevel", indexingSegment.topLevel,
		"indexingLevel0TailSlot", *level0Index.tailSlot,
		"indexingLevel1TailSlot", *level1Index.tailSlot,
		"indexingLevel2TailSlot", *level2Index.tailSlot)
	return nil
}
