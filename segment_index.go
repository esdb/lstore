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
	ctx countlog.Context, startOffset Offset, filter Filter, cb SearchCallback) error {
	if startOffset >= segment.tailOffset {
		return nil
	}
	return segment.searchForwardAt(ctx, startOffset, filter,
		segment.indexingLevels[segment.topLevel], segment.topLevel, cb)
}

func (segment *indexingSegment) searchForwardAt(
	ctx countlog.Context, startOffset Offset, filter Filter,
	levelIndex *slotIndex, level level, cb SearchCallback) error {
	if level == level0 {
		for slot := biter.Slot(0); slot < levelIndex.tailSlot; slot++ {
			blk, err := segment.blockManager.readBlock(blockSeq(levelIndex.children[slot]))
			ctx.TraceCall("callee!blockManager.readBlock", err)
			if err != nil {
				return err
			}
			err = blk.scanForward(ctx, startOffset, filter, cb)
			if err != nil {
				return err
			}
		}
	} else {
		result := levelIndex.search(level, filter)
		result &= biter.SetBitsForwardUntil[levelIndex.tailSlot]
		iter := result.ScanForward()
		for {
			slot := iter()
			if slot == biter.NotFound {
				return nil
			}
			childLevel := level - 1
			childSlotIndexSeq := slotIndexSeq(levelIndex.children[slot])
			childSlotIndex, err := segment.slotIndexManager.readSlotIndex(childSlotIndexSeq, childLevel)
			ctx.TraceCall("callee!slotIndexManager.mapWritableSlotIndex", err)
			if err != nil {
				return err
			}
			err = segment.searchForwardAt(ctx, startOffset, filter, childSlotIndex, childLevel, cb)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
