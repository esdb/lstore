package lstore

import (
	"github.com/esdb/lstore/ref"
	"github.com/esdb/biter"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"os"
	"path"
)

type indexedSegmentVersion struct {
	segmentHeader
	tailSeq          RowSeq
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
}

type indexedSegment struct {
	indexedSegmentVersion
	*ref.ReferenceCounted
	topLevel int // minimum 3 level
	levels   []*indexingSegment
}

type indexingSegmentVersion struct {
	segmentHeader
	slotIndex slotIndex
	tailSlot  biter.Slot
}

type indexingSegment struct {
	indexingSegmentVersion
	*ref.ReferenceCounted
}

func openIndexedSegment(path string) (*indexedSegment, error) {
	buf, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		if err := initIndexedSegment(path); err != nil {
			return nil, err
		}
		buf, err = ioutil.ReadFile(path)
	}
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	segment, _ := iter.Unmarshal((*indexedSegmentVersion)(nil)).(*indexedSegmentVersion)
	if iter.Error != nil {
		return nil, iter.Error
	}
	return &indexedSegment{
		indexedSegmentVersion: *segment,
		ReferenceCounted:      ref.NewReferenceCounted("indexed segment")}, nil
}

func initIndexedSegment(segmentPath string) error {
	stream := gocodec.NewStream(nil)
	stream.Marshal(indexedSegmentVersion{
		segmentHeader: segmentHeader{segmentType: SegmentTypeIndexed},
	})
	if stream.Error != nil {
		return stream.Error
	}
	os.MkdirAll(path.Dir(segmentPath), 0777)
	return ioutil.WriteFile(segmentPath, stream.Buffer(), 0666)
}

func (segment *indexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	return iterateChunks(nil)
}
