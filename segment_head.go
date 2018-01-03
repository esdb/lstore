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

const levelsCount = 9
const level0 level = 0 // small
const level1 level = 1 // medium
const level2 level = 2 // large
type level int

type headSegmentVersion struct {
	segmentHeader
	tailOffset       Offset
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
	topLevel         level          // minimum 3 level
	levels           []slotIndexSeq // total 9 levels
}

type headSegment struct {
	*headSegmentVersion
	*ref.ReferenceCounted
	writeMMap        mmap.MMap
	levelObjs        []*slotIndex
	blockManager     blockManager
	slotIndexManager slotIndexManager
	strategy         *IndexingStrategy
}

func openHeadSegment(ctx countlog.Context, segmentPath string,
	blockManager blockManager, slotIndexManager slotIndexManager) (*headSegment, error) {
	file, err := os.OpenFile(segmentPath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = initHeadSegment(ctx, segmentPath, slotIndexManager)
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
	segment, _ := iter.Unmarshal((*headSegmentVersion)(nil)).(*headSegmentVersion)
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
	return &headSegment{
		headSegmentVersion: segment,
		ReferenceCounted:   ref.NewReferenceCounted("indexed segment", resources...),
		writeMMap:          writeMMap,
		levelObjs:          editingLevels,
		blockManager:       blockManager,
		slotIndexManager:   slotIndexManager,
		strategy:           slotIndexManager.indexingStrategy(),
	}, nil
}

func initHeadSegment(ctx countlog.Context, segmentPath string, slotIndexManager slotIndexManager) (*os.File, error) {
	levels := make([]slotIndexSeq, levelsCount)
	tailSlotIndexSeq := slotIndexSeq(0)
	for i := level0; i <= level2; i++ {
		var err error
		var slotIndex *slotIndex
		levels[i], tailSlotIndexSeq, slotIndex, err = slotIndexManager.newSlotIndex(tailSlotIndexSeq, i)
		if err != nil {
			return nil, err
		}
		if i > level0 {
			slotIndex.children[0] = uint64(levels[i-1])
		}
		err = slotIndexManager.updateChecksum(levels[i], i)
		if err != nil {
			return nil, err
		}
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(headSegmentVersion{
		segmentHeader:    segmentHeader{segmentType: segmentTypeHead},
		topLevel:         level2,
		levels:           levels,
		tailSlotIndexSeq: tailSlotIndexSeq,
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

func (segment *headSegment) addBlock(ctx countlog.Context, blk *block) error {
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
	level0SlotIndex := segment.levelObjs[0]
	level1SlotIndex := segment.levelObjs[1]
	level2SlotIndex := segment.levelObjs[2]
	level0SlotIndex.children[slots[0]] = uint64(blockSeq)
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
				parentPbf := segment.levelObjs[j].pbfs[i]
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

func (segment *headSegment) nextSlot(ctx countlog.Context) ([]biter.Slot, error) {
	slots := make([]biter.Slot, 9)
	cnt := int(segment.tailOffset) >> blockLengthInPowerOfTwo
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
	if level0Slot != 0 || segment.tailOffset == 0 {
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

func (segment *headSegment) rotate(level level, slot biter.Slot) (err error) {
	if level+1 > segment.topLevel {
		segment.topLevel = level + 1
		slotIndexSeq, nextSlotIndexSeq, slotIndex, err := segment.slotIndexManager.newSlotIndex(
			segment.tailSlotIndexSeq, level)
		if err != nil {
			return err
		}
		segment.tailSlotIndexSeq = nextSlotIndexSeq
		segment.levels[level+1] = slotIndexSeq
		segment.levelObjs[level+1] = slotIndex
		slotIndex.updateSlot(biter.SetBits[0], segment.levelObjs[level])
	}
	parentLevel := segment.levelObjs[level+1]
	parentLevel.children[slot] = uint64(segment.levels[level])
	slotIndexManager := segment.slotIndexManager
	if err := slotIndexManager.updateChecksum(segment.levels[level], level); err != nil {
		return err
	}
	segment.levels[level], segment.tailSlotIndexSeq, segment.levelObjs[level], err = slotIndexManager.newSlotIndex(
		segment.tailSlotIndexSeq, level)
	return
}

func (segment *headSegment) searchForward(
	ctx countlog.Context, startOffset Offset, filters []Filter, cb SearchCallback) error {
	return segment.searchForwardAt(ctx, startOffset, filters, cb,
		segment.levelObjs[segment.topLevel], segment.topLevel)
}

func (segment *headSegment) searchForwardAt(
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
