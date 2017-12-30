package lstore

import (
	"github.com/esdb/lstore/ref"
	"github.com/esdb/biter"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"os"
	"path"
	"fmt"
	"io"
	"github.com/edsrzf/mmap-go"
)

type indexedSegmentVersion struct {
	segmentHeader
	topLevel         int // minimum 3 level
	tailSeq          RowSeq
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
}

type indexedSegment struct {
	indexedSegmentVersion
	*ref.ReferenceCounted
	levels []*indexingSegment
}

type indexingSegmentVersion struct {
	segmentHeader
	slotIndex *slotIndex
	tailSlot  biter.Slot
}

type indexingSegment struct {
	indexingSegmentVersion
	*ref.ReferenceCounted
}

func openIndexedSegment(path string, strategy *indexingStrategy) (*indexedSegment, error) {
	buf, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		if err := initIndexedSegment(path, strategy); err != nil {
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
	var rootResources []io.Closer
	var levels []*indexingSegment
	for i := 0; i < segment.topLevel; i++ {
		var resources []io.Closer
		file, err := os.OpenFile(fmt.Sprintf("%s.level%d", path, i), os.O_RDONLY, 0666)
		if err != nil {
			return nil, err
		}
		resources = append(resources, file)
		readMMap, err := mmap.Map(file, mmap.COPY, 0)
		if err != nil {
			return nil, err
		}
		resources = append(resources, ref.NewResource("readMMap", func() error {
			return readMMap.Unmap()
		}))
		iter := gocodec.NewIterator(readMMap)
		levelVersion, _ := iter.Unmarshal((*indexingSegmentVersion)(nil)).(*indexingSegmentVersion)
		if iter.Error != nil {
			return nil, iter.Error
		}
		level := &indexingSegment{
			indexingSegmentVersion: *levelVersion,
			ReferenceCounted: ref.NewReferenceCounted(
				fmt.Sprintf("indexing segment level %d", i), resources...),
		}
		levels = append(levels, level)
		rootResources = append(rootResources, level)
	}
	return &indexedSegment{
		indexedSegmentVersion: *segment,
		ReferenceCounted:      ref.NewReferenceCounted("indexed segment", rootResources...),
		levels:                levels,
	}, nil
}

func initIndexedSegment(segmentPath string, strategy *indexingStrategy) error {
	stream := gocodec.NewStream(nil)
	stream.Marshal(indexedSegmentVersion{
		segmentHeader: segmentHeader{segmentType: SegmentTypeIndexed},
		topLevel:      3,
	})
	if stream.Error != nil {
		return stream.Error
	}
	os.MkdirAll(path.Dir(segmentPath), 0777)
	err := ioutil.WriteFile(segmentPath, stream.Buffer(), 0666)
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		stream.Reset(nil)
		stream.Marshal(indexingSegmentVersion{
			segmentHeader: segmentHeader{segmentType: SegmentTypeIndexing},
			tailSlot:      0,
			slotIndex:     newSlotIndex(strategy, strategy.hashingStrategy(i)),
		})
		if stream.Error != nil {
			return stream.Error
		}
		err := ioutil.WriteFile(fmt.Sprintf("%s.level%d", segmentPath, i), stream.Buffer(), 0666)
		if err != nil {
			return err
		}
	}
	return nil
}

func (segment *indexedSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	return iterateChunks(nil)
}
