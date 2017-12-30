package lstore

import (
	"github.com/esdb/lstore/ref"
	"github.com/esdb/biter"
)

type indexedSegmentVersion struct {
	segmentHeader
	tailSeq          RowSeq
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
}

type indexedSegment struct {
	indexedSegmentVersion
	*ref.ReferenceCounted
	topLevel int // minimum 3 level
	levels   []*indexingSegment
}

type indexingSegmentVersion struct {
	segmentHeader
	slotIndex slotIndex
	tailSlot  biter.Slot
}

type indexingSegment struct {
	indexingSegmentVersion
	*ref.ReferenceCounted
}

func openIndexedSegment(path string) (*indexedSegment, error) {
	return &indexedSegment{ReferenceCounted: ref.NewReferenceCounted("indexed segment")}, nil
}

func (segment *indexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	return iterateChunks(nil)
}
