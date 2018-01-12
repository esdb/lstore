package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
)

func (segment *indexSegment) searchForward(
	ctx countlog.Context, slotIndexReader slotIndexReader, blockReader blockReader, req *SearchRequest) error {
	if req.StartOffset >= segment.tailOffset {
		return nil
	}
	slotIndex, err := slotIndexReader.readSlotIndex(segment.levels[segment.topLevel], segment.topLevel)
	if err != nil {
		return err
	}
	return segment.searchForwardAt(ctx, slotIndexReader, blockReader, req, slotIndex, segment.topLevel)
}

func (segment *indexSegment) searchForwardAt(
	ctx countlog.Context, slotIndexReader slotIndexReader, blockReader blockReader, req *SearchRequest,
	levelIndex *slotIndex, level level) error {
	result := levelIndex.search(level, req.Filter)
	result &= biter.SetBitsForwardUntil[*levelIndex.tailSlot]
	iter := result.ScanForward()
	if level == level0 {
		for {
			slot := iter()
			if slot == biter.NotFound {
				return nil
			}
			blkSeq := blockSeq(levelIndex.children[slot])
			if blkSeq == 0 {
				return nil
			}
			blk, err := blockReader.readBlock(blkSeq)
			ctx.TraceCall("callee!blockManager.readBlock", err)
			if err != nil {
				return err
			}
			err = blk.scanForward(ctx, req)
			if err != nil {
				return err
			}
		}
	} else {
		for {
			slot := iter()
			if slot == biter.NotFound {
				return nil
			}
			childLevel := level - 1
			childSlotIndexSeq := slotIndexSeq(levelIndex.children[slot])
			childSlotIndex, err := slotIndexReader.readSlotIndex(childSlotIndexSeq, childLevel)
			ctx.TraceCall("callee!slotIndexManager.mapWritableSlotIndex", err)
			if err != nil {
				return err
			}
			err = segment.searchForwardAt(ctx, slotIndexReader, blockReader, req, childSlotIndex, childLevel)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
