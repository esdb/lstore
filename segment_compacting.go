package lstore

import (
	"os"
	"github.com/esdb/lstore/ref"
	"io"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
)

type compactingSegmentValue struct {
	SegmentHeader
	tailBlockSeq BlockSeq
}

type compactingSegment struct {
	compactingSegmentValue
	*ref.ReferenceCounted
	path string
}

func openCompactingSegment(path string) (*compactingSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{file}
	segment := &compactingSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	resources = append(resources, ref.NewResource("readMMap", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	storage, _ := iter.Unmarshal((*compactingSegmentValue)(nil)).(*compactingSegmentValue)
	if iter.Error != nil {
		readMMap.Unmap()
		file.Close()
		return nil, iter.Error
	}
	segment.path = path
	segment.compactingSegmentValue = *storage
	segment.ReferenceCounted = ref.NewReferenceCounted("compacting segment", resources...)
	return segment, nil
}

func createCompactingSegment(path string, segment compactingSegmentValue) (*compactingSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	stream := gocodec.NewStream(nil)
	stream.Marshal(segment)
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
	if err != nil {
		return nil, err
	}
	return &compactingSegment{
		path: path,
		compactingSegmentValue: segment,
		ReferenceCounted:       ref.NewReferenceCounted("compacting segment"),
	}, nil
}