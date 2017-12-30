package lstore

import (
	"github.com/esdb/biter"
	"github.com/esdb/lstore/ref"
)

// relativeOffset relative to the head of compacted segment
type relativeOffset uint64

type compactedSegmentVersion struct {
	segmentHeader
	tailSeq              RowSeq
	slotIndex            slotIndex
	tailSlot             biter.Slot
	children             []relativeOffset // 64 slots
	tailBlockIndexOffset relativeOffset
}

type compactedSegment struct {
	compactedSegmentVersion
	*ref.ReferenceCounted
}

type rootCompactedSegmentVersion struct {
	segmentHeader
	tailSeq   RowSeq
	slotIndex slotIndex
	tailSlot  biter.Slot
}