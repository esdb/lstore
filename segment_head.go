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
)

const levelsCount = 9
const level0 level = 0 // small
const level1 level = 1 // medium
const level2 level = 2 // large
type level int

type headSegmentVersion struct {
	segmentHeader
	headOffset       Offset
	tailOffset       Offset
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
	topLevel         level          // minimum 3 level
	levels           []slotIndexSeq // total 9 levels
}

type headSegment struct {
	*headSegmentVersion
	*ref.ReferenceCounted
	writeMMap     mmap.MMap
	editingLevels []*slotIndex
}

// TODO: merge editingHead into headSegment
type editingHead struct {
	*headSegmentVersion
	writeBlock       func(blockSeq, *block) (blockSeq, blockSeq, error)
	slotIndexManager *slotIndexManager
	strategy         *IndexingStrategy
	editingLevels    []*slotIndex
}

func openHeadSegment(ctx countlog.Context, segmentPath string, slotIndexManager *slotIndexManager) (*headSegment, error) {
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
		if editingLevels[i], err = slotIndexManager.mapWritableSlotIndex(segment.levels[i], i); err != nil {
			return nil, err
		}
	}
	return &headSegment{
		headSegmentVersion: segment,
		ReferenceCounted:   ref.NewReferenceCounted("indexed segment", resources...),
		writeMMap:          writeMMap,
		editingLevels:      editingLevels,
	}, nil
}

func initHeadSegment(ctx countlog.Context, segmentPath string, slotIndexManager *slotIndexManager) (*os.File, error) {
	levels := make([]slotIndexSeq, levelsCount)
	tailSlotIndexSeq := slotIndexSeq(0)
	for i := level0; i <= level2; i++ {
		var err error
		levels[i], tailSlotIndexSeq, _, err = slotIndexManager.newSlotIndex(tailSlotIndexSeq, i)
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

func (editing *editingHead) addBlock(ctx countlog.Context, blk *block) error {
	var err error
	slots, err := editing.nextSlot(ctx)
	level0SlotMask := biter.SetBits[slots[level0]]
	level1SlotMask := biter.SetBits[slots[level1]]
	level2SlotMask := biter.SetBits[slots[level2]]
	if err != nil {
		return err
	}
	// hash will update block, so call it before write
	blkHash := blk.Hash(editing.strategy)
	var blockSeq blockSeq
	blockSeq, editing.tailBlockSeq, err = editing.writeBlock(editing.tailBlockSeq, blk)
	ctx.TraceCall("callee!editing.writeBlock", err)
	if err != nil {
		return err
	}
	level0SlotIndex := editing.editingLevels[0]
	level1SlotIndex := editing.editingLevels[1]
	level2SlotIndex := editing.editingLevels[2]
	level0SlotIndex.children[slots[0]] = uint64(blockSeq)
	smallHashingStrategy := editing.strategy.smallHashingStrategy
	mediumHashingStrategy := editing.strategy.mediumHashingStrategy
	largeHashingStrategy := editing.strategy.largeHashingStrategy
	for i, hashColumn := range blkHash {
		pbf0 := level0SlotIndex.pbfs[i]
		pbf1 := level1SlotIndex.pbfs[i]
		pbf2 := level2SlotIndex.pbfs[i]
		for _, hashedElement := range hashColumn {
			// level0, level1, level2 are computed from block hash
			pbf0.Put(level0SlotMask, smallHashingStrategy.HashStage2(hashedElement))
			pbf1.Put(level1SlotMask, mediumHashingStrategy.HashStage2(hashedElement))
			locations := largeHashingStrategy.HashStage2(hashedElement)
			pbf2.Put(level2SlotMask, locations)
			// from level3 to levelN, they are derived from level2
			for j := level(3); j <= editing.topLevel; j++ {
				parentPbf := editing.editingLevels[j].pbfs[i]
				levelNMask := biter.SetBits[slots[j]]
				for _, location := range locations {
					parentPbf[location] |= levelNMask
				}
			}
		}
	}
	editing.tailOffset += Offset(blockLength)
	return nil
}

func (editing *editingHead) nextSlot(ctx countlog.Context) ([]biter.Slot, error) {
	slots := make([]biter.Slot, 9)
	cnt := int(editing.tailOffset) >> blockLengthInPowerOfTwo
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
	if level0Slot != 0 || editing.tailOffset == 0 {
		return slots, nil
	}
	if level1Slot != 0 {
		if err := editing.rotate(level0, level1Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(level0, 63); err != nil {
		return nil, err
	}
	if level2Slot != 0 {
		if err := editing.rotate(level1, level2Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(level1, 63); err != nil {
		return nil, err
	}
	if level3Slot != 0 {
		if err := editing.rotate(level2, level3Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(level2, 63); err != nil {
		return nil, err
	}
	if level4Slot != 0 {
		if err := editing.rotate(3, level4Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(3, 63); err != nil {
		return nil, err
	}
	if level5Slot != 0 {
		if err := editing.rotate(4, level5Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(4, 63); err != nil {
		return nil, err
	}
	if level6Slot != 0 {
		if err := editing.rotate(5, level6Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(5, 63); err != nil {
		return nil, err
	}
	if level7Slot != 0 {
		if err := editing.rotate(6, level7Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	if err := editing.rotate(6, 63); err != nil {
		return nil, err
	}
	if level8Slot != 0 {
		if err := editing.rotate(7, level8Slot-1); err != nil {
			return nil, err
		}
		return slots, nil
	}
	panic("bloom filter tree exceed capacity")
}

func (editing *editingHead) rotate(level level, slot biter.Slot) (err error) {
	if level+1 > editing.topLevel {
		editing.topLevel = level + 1
		slotIndexSeq, nextSlotIndexSeq, slotIndex, err := editing.slotIndexManager.newSlotIndex(
			editing.tailSlotIndexSeq, level)
		if err != nil {
			return err
		}
		editing.tailSlotIndexSeq = nextSlotIndexSeq
		editing.levels[level+1] = slotIndexSeq
		editing.editingLevels[level+1] = slotIndex
		slotIndex.updateSlot(biter.SetBits[0], editing.editingLevels[level])
	}
	parentLevel := editing.editingLevels[level+1]
	parentLevel.children[slot] = uint64(editing.levels[level])
	slotIndexManager := editing.slotIndexManager
	if err := slotIndexManager.updateChecksum(editing.levels[level], level); err != nil {
		return err
	}
	editing.levels[level], editing.tailSlotIndexSeq, editing.editingLevels[level], err = slotIndexManager.newSlotIndex(
		editing.tailSlotIndexSeq, level)
	return
}

func (segment *headSegmentVersion) scanForward(
	ctx countlog.Context, blockManager *blockManager, slotIndexManager *slotIndexManager,
	filters ...Filter) chunkIterator {
	return iterateChunks(nil)
}
