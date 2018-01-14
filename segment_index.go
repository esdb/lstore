package lstore

import (
	"github.com/v2pro/plz/countlog"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"fmt"
	"path"
	"os"
)

const firstBlockSeq = 1
const firstSlotIndexSeq = 1
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

func newIndexSegment(slotIndexWriter slotIndexWriter, prev *indexSegment) (*indexSegment, error) {
	levels := make([]slotIndexSeq, levelsCount)
	tailSlotIndexSeq := slotIndexSeq(firstSlotIndexSeq)
	tailBlockSeq := blockSeq(firstBlockSeq)
	headOffset := Offset(firstOffset)
	if prev != nil {
		tailSlotIndexSeq = prev.tailSlotIndexSeq
		tailBlockSeq = prev.tailBlockSeq
		headOffset = prev.tailOffset
	}
	for i := level0; i <= level2; i++ {
		var err error
		var slotIndex *slotIndex
		levels[i], tailSlotIndexSeq, slotIndex, err = slotIndexWriter.newSlotIndex(tailSlotIndexSeq, i)
		if err != nil {
			return nil, err
		}
		if i > level0 {
			slotIndex.children[0] = uint64(levels[i-1])
			slotIndex.setTailSlot(1)
		}
	}
	return &indexSegment{
		segmentHeader:    segmentHeader{segmentType: segmentTypeIndex, headOffset: headOffset},
		topLevel:         level2,
		levels:           levels,
		tailSlotIndexSeq: tailSlotIndexSeq,
		tailBlockSeq:     tailBlockSeq,
		tailOffset:       headOffset,
	}, nil
}

func openIndexSegment(ctx countlog.Context, indexedSegmentPath string) (*indexSegment, error) {
	buf, err := ioutil.ReadFile(indexedSegmentPath)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	segment, _ := iter.Unmarshal((*indexSegment)(nil)).(*indexSegment)
	if iter.Error != nil {
		return nil, fmt.Errorf("unmarshal index segment failed: %s", iter.Error.Error())
	}
	return segment, nil
}

func createIndexSegment(ctx countlog.Context, segmentPath string, segment *indexSegment) error {
	stream := gocodec.NewStream(nil)
	stream.Marshal(*segment)
	ctx.TraceCall("callee!stream.Marshal", stream.Error)
	if stream.Error != nil {
		return stream.Error
	}
	os.MkdirAll(path.Dir(segmentPath), 0777)
	err := ioutil.WriteFile(segmentPath, stream.Buffer(), 0666)
	ctx.DebugCall("callee!createIndexSegment", err, "segmentPath", segmentPath,
		"headOffset", segment.headOffset, "tailOffset", segment.tailOffset)
	if err != nil {
		return err
	}
	return nil
}

func (segment *indexSegment) copy() *indexSegment {
	copied := *segment
	copied.levels = append([]slotIndexSeq(nil), copied.levels...)
	return &copied
}