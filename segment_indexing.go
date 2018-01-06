package lstore

import (
	"github.com/esdb/lstore/ref"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"os"
	"path"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"github.com/esdb/biter"
	"github.com/esdb/pbloom"
)

type indexingSegment struct {
	*searchable
	*ref.ReferenceCounted
	writeMMap        mmap.MMap
	indexingLevels   []*slotIndex
	slotIndexManager slotIndexManager
	strategy         *IndexingStrategy
}

func openIndexingSegment(ctx countlog.Context, segmentPath string, prev *indexSegment,
	blockManager blockManager, slotIndexManager slotIndexManager) (*indexingSegment, error) {
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = initIndexingSegment(ctx, segmentPath, prev, slotIndexManager)
	}
	ctx.TraceCall("callee!os.OpenFile", err)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	ctx.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return nil, err
	}
	resources = append(resources, plz.WrapCloser(writeMMap.Unmap))
	iter := gocodec.NewIterator(writeMMap)
	segment, _ := iter.Unmarshal((*indexSegment)(nil)).(*indexSegment)
	ctx.TraceCall("callee!iter.Unmarshal", iter.Error)
	if iter.Error != nil {
		return nil, iter.Error
	}
	gocodec.UpdateChecksum(writeMMap)
	if err := writeMMap.Flush(); err != nil {
		return nil, err
	}
	editingLevels := make([]*slotIndex, levelsCount)
	for i := level0; i <= segment.topLevel; i++ {
		editingLevels[i], err = slotIndexManager.mapWritableSlotIndex(segment.levels[i], i)
		ctx.TraceCall("callee!slotIndexManager.mapWritableSlotIndex", err, "level", i)
		if err != nil {
			return nil, err
		}
	}
	return &indexingSegment{
		ReferenceCounted: ref.NewReferenceCounted("indexing segment", resources...),
		searchable:     &searchable{
			indexSegment: segment,
			blockManager: blockManager,
			readSlotIndex: slotIndexManager.mapWritableSlotIndex,
		},
		writeMMap:        writeMMap,
		indexingLevels:   editingLevels,
		slotIndexManager: slotIndexManager,
		strategy:         slotIndexManager.indexingStrategy(),
	}, nil
}

func initIndexingSegment(ctx countlog.Context, segmentPath string, prev *indexSegment,
	slotIndexManager slotIndexManager) (*os.File, error) {
	levels := make([]slotIndexSeq, levelsCount)
	tailSlotIndexSeq := slotIndexSeq(0)
	tailBlockSeq := blockSeq(0)
	startOffset := Offset(0)
	if prev != nil {
		tailSlotIndexSeq = prev.tailSlotIndexSeq
		tailBlockSeq = prev.tailBlockSeq
		startOffset = prev.tailOffset
	}
	for i := level0; i <= level2; i++ {
		var err error
		var slotIndex *slotIndex
		levels[i], tailSlotIndexSeq, slotIndex, err = slotIndexManager.newSlotIndex(tailSlotIndexSeq, i)
		if err != nil {
			return nil, err
		}
		if i > level0 {
			slotIndex.children[0] = uint64(levels[i-1])
			slotIndex.tailSlot = 1
		}
		err = slotIndexManager.updateChecksum(levels[i], i)
		if err != nil {
			return nil, err
		}
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(indexSegment{
		segmentHeader:    segmentHeader{segmentType: segmentTypeHead, startOffset: startOffset},
		topLevel:         level2,
		levels:           levels,
		tailSlotIndexSeq: tailSlotIndexSeq,
		tailBlockSeq: tailBlockSeq,
		tailOffset: startOffset,
	})
	ctx.TraceCall("callee!stream.Marshal", stream.Error)
	if stream.Error != nil {
		return nil, stream.Error
	}
	os.MkdirAll(path.Dir(segmentPath), 0777)
	err := ioutil.WriteFile(segmentPath, stream.Buffer(), 0666)
	ctx.TraceCall("callee!ioutil.WriteFile", err)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(segmentPath, os.O_RDWR, 0666)
}

func (segment *indexingSegment) addBlock(ctx countlog.Context, blk *block) error {
	var err error
	slots, err := segment.nextSlot(ctx)
	level0SlotMask := biter.SetBits[slots[level0]]
	level1SlotMask := biter.SetBits[slots[level1]]
	level2SlotMask := biter.SetBits[slots[level2]]
	if err != nil {
		return err
	}
	// hash will update block, so call it before write
	blkHash := blk.Hash(segment.strategy)
	var blockSeq blockSeq
	blockSeq, segment.tailBlockSeq, err = segment.blockManager.writeBlock(segment.tailBlockSeq, blk)
	ctx.TraceCall("callee!segment.writeBlock", err)
	if err != nil {
		return err
	}
	level0SlotIndex := segment.indexingLevels[0]
	level1SlotIndex := segment.indexingLevels[1]
	level2SlotIndex := segment.indexingLevels[2]
	level0SlotIndex.children[slots[0]] = uint64(blockSeq)
	level0SlotIndex.tailSlot = slots[0] + 1
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
				parentPbf := segment.indexingLevels[j].pbfs[i]
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

func (segment *indexingSegment) nextSlot(ctx countlog.Context) ([]biter.Slot, error) {
	slots := make([]biter.Slot, 9)
	cnt := int(segment.tailOffset - segment.startOffset) >> blockLengthInPowerOfTwo
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
	if level0Slot != 0 || (segment.tailOffset - segment.startOffset) == 0 {
		return slots, nil
	}
	if level1Slot != 0 {
		if err := segment.rotate(level0, level1Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(level0, 63); err != nil {
		return nil, err
	}
	if level2Slot != 0 {
		if err := segment.rotate(level1, level2Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(level1, 63); err != nil {
		return nil, err
	}
	if level3Slot != 0 {
		if err := segment.rotate(level2, level3Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(level2, 63); err != nil {
		return nil, err
	}
	if level4Slot != 0 {
		if err := segment.rotate(3, level4Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(3, 63); err != nil {
		return nil, err
	}
	if level5Slot != 0 {
		if err := segment.rotate(4, level5Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(4, 63); err != nil {
		return nil, err
	}
	if level6Slot != 0 {
		if err := segment.rotate(5, level6Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(5, 63); err != nil {
		return nil, err
	}
	if level7Slot != 0 {
		if err := segment.rotate(6, level7Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := segment.rotate(6, 63); err != nil {
		return nil, err
	}
	if level8Slot != 0 {
		if err := segment.rotate(7, level8Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	panic("bloom filter tree exceed capacity")
}

func (segment *indexingSegment) rotate(level level, slot biter.Slot) (err error) {
	if level+1 > segment.topLevel {
		segment.topLevel = level + 1
		slotIndexSeq, nextSlotIndexSeq, slotIndex, err := segment.slotIndexManager.newSlotIndex(
			segment.tailSlotIndexSeq, level)
		if err != nil {
			return err
		}
		segment.tailSlotIndexSeq = nextSlotIndexSeq
		segment.levels[level+1] = slotIndexSeq
		segment.indexingLevels[level+1] = slotIndex
		slotIndex.updateSlot(biter.SetBits[0], segment.indexingLevels[level])
		slotIndex.tailSlot = 1
	}
	parentLevel := segment.indexingLevels[level+1]
	parentLevel.children[slot] = uint64(segment.levels[level])
	parentLevel.tailSlot = slot + 1
	slotIndexManager := segment.slotIndexManager
	if err := slotIndexManager.updateChecksum(segment.levels[level], level); err != nil {
		return err
	}
	segment.levels[level], segment.tailSlotIndexSeq, segment.indexingLevels[level], err = slotIndexManager.newSlotIndex(
		segment.tailSlotIndexSeq, level)
	return
}
