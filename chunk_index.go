package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
)

// indexChunk makes the indexSegment searchable
type indexChunk struct {
	*indexSegment
	blockManager blockManager
	// readSlotIndex might read from rwCache or roCache, depending on the segment is indexing or fully indexed
	// TODO: always use roCache when gocodec supported MinimumCopy
	readSlotIndex func(slotIndexSeq, level) (*slotIndex, error)
}

func (chunk *indexChunk) searchForward(
	ctx countlog.Context, startOffset Offset, filter Filter, cb SearchCallback) error {
	if startOffset >= chunk.tailOffset {
		return nil
	}
	slotIndex, err := chunk.readSlotIndex(chunk.levels[chunk.topLevel], chunk.topLevel)
	if err != nil {
		return err
	}
	return chunk.searchForwardAt(ctx, startOffset, filter, slotIndex, chunk.topLevel, cb)
}

func (chunk *indexChunk) searchForwardAt(
	ctx countlog.Context, startOffset Offset, filter Filter,
	levelIndex *slotIndex, level level, cb SearchCallback) error {
	if level == level0 {
		for slot := biter.Slot(0); slot < levelIndex.tailSlot; slot++ {
			blk, err := chunk.blockManager.readBlock(blockSeq(levelIndex.children[slot]))
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
			childSlotIndex, err := chunk.readSlotIndex(childSlotIndexSeq, childLevel)
			ctx.TraceCall("callee!slotIndexManager.mapWritableSlotIndex", err)
			if err != nil {
				return err
			}
			err = chunk.searchForwardAt(ctx, startOffset, filter, childSlotIndex, childLevel, cb)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
