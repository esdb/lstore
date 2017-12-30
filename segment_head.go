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

const level0 level = 0
type level int
type headSegmentVersion struct {
	segmentHeader
	topLevel         level // minimum 3 level
	headOffset       Offset
	tailOffset       Offset
	tailBlockSeq     blockSeq
	tailSlotIndexSeq slotIndexSeq
}

type headSegment struct {
	headSegmentVersion
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

func openHeadSegment(config *Config, strategy *indexingStrategy) (*headSegment, error) {
	headSegmentPath := config.HeadSegmentPath()
	buf, err := ioutil.ReadFile(headSegmentPath)
	if os.IsNotExist(err) {
		if err := initIndexedSegment(headSegmentPath, strategy); err != nil {
			return nil, err
		}
		buf, err = ioutil.ReadFile(headSegmentPath)
	}
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(buf)
	segment, _ := iter.Unmarshal((*headSegmentVersion)(nil)).(*headSegmentVersion)
	if iter.Error != nil {
		return nil, iter.Error
	}
	var rootResources []io.Closer
	var levels []*indexingSegment
	for level := level0; level < segment.topLevel; level++ {
		var resources []io.Closer
		indexingSegmentPath := config.IndexingSegmentPath(level)
		file, err := os.OpenFile(indexingSegmentPath, os.O_RDONLY, 0666)
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
				fmt.Sprintf("indexing segment level %d", level), resources...),
		}
		levels = append(levels, level)
		rootResources = append(rootResources, level)
	}
	return &headSegment{
		headSegmentVersion: *segment,
		ReferenceCounted:   ref.NewReferenceCounted("indexed segment", rootResources...),
		levels:             levels,
	}, nil
}

func initIndexedSegment(segmentPath string, strategy *indexingStrategy) error {
	stream := gocodec.NewStream(nil)
	stream.Marshal(headSegmentVersion{
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

func (segment *headSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	return iterateChunks(nil)
}
