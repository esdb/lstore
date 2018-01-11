package lstore

import (
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
)

func (segment *indexSegment) addBlock(ctx countlog.Context, slotIndexWriter slotIndexWriter, blockWriter blockWriter,
	blk *block) error {
	var err error
	slots, err := segment.nextSlot(ctx, slotIndexWriter)
	level0SlotMask := biter.SetBits[slots[level0]]
	level1SlotMask := biter.SetBits[slots[level1]]
	level2SlotMask := biter.SetBits[slots[level2]]
	if err != nil {
		return err
	}
	// hash will update block, so call it before write
	blkHash := blk.Hash(slotIndexWriter.indexingStrategy())
	var blockSeq blockSeq
	blockSeq, segment.tailBlockSeq, err = blockWriter.writeBlock(segment.tailBlockSeq, blk)
	ctx.TraceCall("callee!segment.writeBlock", err)
	if err != nil {
		return err
	}
	indices := make([]*slotIndex, levelsCount)
	for i := level0; i <= segment.topLevel; i++ {
		indices[i], err = slotIndexWriter.mapWritableSlotIndex(segment.levels[i], i)
		if err != nil {
			return err
		}
	}
	level0SlotIndex := indices[0]
	level1SlotIndex := indices[1]
	level2SlotIndex := indices[2]
	level0SlotIndex.children[slots[0]] = uint64(blockSeq)
	level0SlotIndex.setTailSlot(slots[0] + 1)
	for i, hashColumn := range blkHash {
		pbf0 := level0SlotIndex.pbfs[i]
		pbf1 := level1SlotIndex.pbfs[i]
		pbf2 := level2SlotIndex.pbfs[i]
		for _, hashedElement := range hashColumn {
			// level0, level1, level2 are computed from block hash
			locations := pbloom.BatchPut(hashedElement,
				level0SlotMask, level1SlotMask, level2SlotMask,
				pbf0, pbf1, pbf2)
			// from level3 to levelN, they are derived from level2
			for j := level(3); j <= segment.topLevel; j++ {
				parentPbf := indices[j].pbfs[i]
				levelNMask := biter.SetBits[slots[j]]
				parentPbf[locations[0]] |= levelNMask
				parentPbf[locations[1]] |= levelNMask
				parentPbf[locations[2]] |= levelNMask
				parentPbf[locations[3]] |= levelNMask
			}
		}
	}
	segment.tailOffset += Offset(blockLength)
	return nil
}

func (segment *indexSegment) nextSlot(ctx countlog.Context, slotIndexWriter slotIndexWriter) ([]biter.Slot, error) {
	slots := make([]biter.Slot, 9)
	cnt := int(segment.tailOffset-segment.headOffset) >> blockLengthInPowerOfTwo
	level0Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level1Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level2Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level3Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level4Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level5Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level6Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level7Slot := biter.Slot(cnt % 64)
	cnt = cnt >> 6
	level8Slot := biter.Slot(cnt % 64)
	slots = []biter.Slot{level0Slot, level1Slot, level2Slot, level3Slot, level4Slot,
		level5Slot, level6Slot, level7Slot, level8Slot}
	if level0Slot != 0 || (segment.tailOffset-segment.headOffset) == 0 {
		return slots, nil
	}
	if level1Slot != 0 {
		if err := segment.rotate(slotIndexWriter, level0, level1Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, level0, 63); err != nil {
		return nil, err
	}
	if level2Slot != 0 {
		if err := segment.rotate(slotIndexWriter, level1, level2Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, level1, 63); err != nil {
		return nil, err
	}
	if level3Slot != 0 {
		if err := segment.rotate(slotIndexWriter, level2, level3Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, level2, 63); err != nil {
		return nil, err
	}
	if level4Slot != 0 {
		if err := segment.rotate(slotIndexWriter, 3, level4Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, 3, 63); err != nil {
		return nil, err
	}
	if level5Slot != 0 {
		if err := segment.rotate(slotIndexWriter, 4, level5Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, 4, 63); err != nil {
		return nil, err
	}
	if level6Slot != 0 {
		if err := segment.rotate(slotIndexWriter, 5, level6Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, 5, 63); err != nil {
		return nil, err
	}
	if level7Slot != 0 {
		if err := segment.rotate(slotIndexWriter, 6, level7Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(slotIndexWriter, 6, 63); err != nil {
		return nil, err
	}
	if level8Slot != 0 {
		if err := segment.rotate(slotIndexWriter, 7, level8Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	panic("bloom filter tree exceed capacity")
}

func (segment *indexSegment) rotate(slotIndexWriter slotIndexWriter, level level, slot biter.Slot) (err error) {
	if level+1 > segment.topLevel {
		segment.topLevel = level + 1
		slotIndexSeq, nextSlotIndexSeq, slotIndex, err := slotIndexWriter.newSlotIndex(
			segment.tailSlotIndexSeq, level)
		if err != nil {
			return err
		}
		segment.tailSlotIndexSeq = nextSlotIndexSeq
		segment.levels[level+1] = slotIndexSeq
		idx, err := slotIndexWriter.mapWritableSlotIndex(segment.levels[level], level)
		if err != nil {
			return err
		}
		slotIndex.updateSlot(biter.SetBits[0], idx)
		slotIndex.setTailSlot(1)
	}
	idx, err := slotIndexWriter.mapWritableSlotIndex(segment.levels[level+1], level+1)
	if err != nil {
		return err
	}
	idx.children[slot] = uint64(segment.levels[level])
	idx.setTailSlot(slot + 1)
	segment.levels[level], segment.tailSlotIndexSeq, _, err = slotIndexWriter.newSlotIndex(
		segment.tailSlotIndexSeq, level)
	return nil
}
