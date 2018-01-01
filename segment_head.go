package lstore

import (
	"github.com/esdb/lstore/ref"
	"io/ioutil"
	"github.com/esdb/gocodec"
	"os"
	"path"
	"fmt"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"github.com/esdb/biter"
)

const level0 level = 0 // small
const level1 level = 1 // medium
const level2 level = 2 // large
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

type editingHead struct {
	*headSegmentVersion
	levels       []*indexingSegment
	editedLevels []*slotIndex
	writeBlock   func(blockSeq, *block) (blockSeq, error)
	strategy     *indexingStrategy
}

type indexingSegmentVersion struct {
	segmentHeader
	slotIndex *slotIndex
}

type indexingSegment struct {
	indexingSegmentVersion
	*ref.ReferenceCounted
}

func openHeadSegment(ctx countlog.Context, config *Config, strategy *indexingStrategy) (*headSegment, error) {
	headSegmentPath := config.HeadSegmentPath()
	buf, err := ioutil.ReadFile(headSegmentPath)
	if os.IsNotExist(err) {
		if err := initIndexedSegment(ctx, config, strategy); err != nil {
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
		ctx.TraceCall("callee!os.OpenFile", err)
		if err != nil {
			return nil, err
		}
		resources = append(resources, file)
		readMMap, err := mmap.Map(file, mmap.COPY, 0)
		if err != nil {
			return nil, err
		}
		resources = append(resources, plz.WrapCloser(readMMap.Unmap))
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

func initIndexedSegment(ctx countlog.Context, config *Config, strategy *indexingStrategy) error {
	stream := gocodec.NewStream(nil)
	stream.Marshal(headSegmentVersion{
		segmentHeader: segmentHeader{segmentType: segmentTypeIndexed},
		topLevel:      3,
	})
	if stream.Error != nil {
		return stream.Error
	}
	segmentPath := config.HeadSegmentPath()
	os.MkdirAll(path.Dir(segmentPath), 0777)
	err := ioutil.WriteFile(segmentPath, stream.Buffer(), 0666)
	if err != nil {
		return err
	}
	for level := level0; level < 3; level++ {
		stream.Reset(nil)
		stream.Marshal(indexingSegmentVersion{
			segmentHeader: segmentHeader{segmentType: segmentTypeIndexing},
			slotIndex:     newSlotIndex(strategy, strategy.hashingStrategy(level)),
		})
		if stream.Error != nil {
			return stream.Error
		}
		err := ioutil.WriteFile(config.IndexingSegmentPath(level), stream.Buffer(), 0666)
		ctx.TraceCall("callee!ioutil.WriteFile", err)
		if err != nil {
			return err
		}
	}
	return nil
}

func saveIndexingSegment(ctx countlog.Context, config *Config, level level, slotIndex *slotIndex) error {
	tmpPath := config.IndexingSegmentTmpPath(level)
	finalPath := config.IndexingSegmentPath(level)
	stream := gocodec.NewStream(nil)
	stream.Marshal(indexingSegmentVersion{
		segmentHeader: segmentHeader{
			segmentType: segmentTypeIndexing,
		},
		slotIndex: slotIndex,
	})
	ctx.TraceCall("callee!stream.Marshal", stream.Error)
	if stream.Error != nil {
		return fmt.Errorf("save indexing segment failed: %v", stream.Error.Error())
	}
	err := ioutil.WriteFile(tmpPath, stream.Buffer(), 0666)
	ctx.TraceCall("callee!ioutil.WriteFile", err)
	if err != nil {
		return err
	}
	err = os.Rename(tmpPath, finalPath)
	ctx.TraceCall("callee!os.Rename", err)
	if err != nil {
		return err
	}
	return nil
}

func (segment *headSegment) edit() (*slotIndex, *slotIndex, *slotIndex) {
	level0Copy := segment.levels[level0].slotIndex.copy()
	level1Copy := segment.levels[level1].slotIndex.copy()
	level2Copy := segment.levels[level2].slotIndex.copy()
	return level0Copy, level1Copy, level2Copy
}

func (editing *editingHead) addBlock(ctx countlog.Context, blk *block) {
	var err error
	blockSeq := editing.tailBlockSeq
	editing.tailBlockSeq, err = editing.writeBlock(blockSeq, blk)
	ctx.TraceCall("callee!editing.writeBlock", err)
	level0SlotIndex := editing.editLevel(level0)
	level0SlotIndex.children = append(level0SlotIndex.children, uint64(blockSeq))
	blkHash := blk.Hash(editing.strategy)
	smallHashingStrategy := editing.strategy.smallHashingStrategy
	for i, hashColumn := range blkHash {
		pbf := level0SlotIndex.pbfs[i]
		for _, hashedElement := range hashColumn {
			pbf.Put(biter.SetBits[0], smallHashingStrategy.HashStage2(hashedElement))
		}
	}
}

func (editing *editingHead) editLevel(level level) *slotIndex {
	if int(level) < len(editing.editedLevels) {
		return editing.editedLevels[level]
	}
	editing.editedLevels = append(editing.editedLevels, editing.levels[len(editing.editedLevels)].slotIndex.copy())
	return editing.editLevel(level)
}

func (segment *headSegment) scanForward(blockManager *blockManager, filters []Filter) chunkIterator {
	return iterateChunks(nil)
}
