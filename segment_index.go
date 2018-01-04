package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
)

const levelsCount = 9
const level0 level = 0 // small
const level1 level = 1 // medium
const level2 level = 2 // large
type level int

// indexSegment can be serialized
type indexSegment struct {
	segmentHeader
	tailOffset       Offset
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
	topLevel         level          // minimum 3 level
	levels           []slotIndexSeq // total 9 levels
}

func (segment *indexingSegment) searchForward(
	ctx countlog.Context, startOffset Offset, filters []Filter, cb SearchCallback) error {
	return segment.searchForwardAt(ctx, startOffset, filters, cb,
		segment.indexingLevels[segment.topLevel], segment.topLevel)
}

func (segment *indexingSegment) searchForwardAt(
	ctx countlog.Context, startOffset Offset, filters []Filter, cb SearchCallback,
	slotIndex *slotIndex, level level) error {
	result := slotIndex.search(level, filters...)
	iter := result.ScanForward()
	for {
		slot := iter()
		if slot == biter.NotFound {
			return nil
		}
		if level == level0 {
			blk, err := segment.blockManager.readBlock(blockSeq(slotIndex.children[slot]))
			if err != nil {
				return err
			}
			err = blk.search(ctx, startOffset, filters, cb)
			if err != nil {
				return err
			}
		} else {
			childLevel := level - 1
			childSlotIndexSeq := slotIndexSeq(slotIndex.children[slot])
			childSlotIndex, err := segment.slotIndexManager.mapWritableSlotIndex(childSlotIndexSeq, childLevel)
			if err != nil {
				return err
			}
			err = segment.searchForwardAt(ctx, startOffset, filters, cb, childSlotIndex, childLevel)
			if err != nil {
				return err
			}
		}
	}
}