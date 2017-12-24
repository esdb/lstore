package lstore

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"os"
	"errors"
	"io"
)

type RawSegment struct {
	SegmentHeader
	AsBlock   Block
	Path      string
	resources []func() error
	Tail      Offset
}

func openRawSegment(path string) (*RawSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	segment := &RawSegment{}
	segment.resources = append(segment.resources, func() error {
		return file.Close()
	})
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as COPY", "err", err, "path", path)
		return nil, err
	}
	segment.resources = append(segment.resources, func() error {
		return readMMap.Unmap()
	})
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Unmarshal((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	segment.Path = path
	segment.AsBlock, err = segment.loadBlock(iter)
	if err != nil {
		return nil, err
	}
	return segment, nil
}

func (segment *RawSegment) Close() error {
	failed := false
	for _, resource := range segment.resources {
		err := resource()
		if err != nil {
			countlog.Error("event!segment.failed to close resource", "err", err)
			failed = true
		}
	}
	if failed {
		return errors.New("not all resources closed properly")
	}
	return nil
}

func (segment *RawSegment) loadBlock(iter *gocodec.Iterator) (*rowBasedBlock, error) {
	var rows []Row
	startOffset := segment.StartOffset
	totalSize := Offset(len(iter.Buffer()))
	for {
		entry, _ := iter.Unmarshal((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			return &rowBasedBlock{rows}, nil
		}
		if iter.Error != nil {
			return nil, iter.Error
		}
		offset := startOffset + (totalSize - Offset(len(iter.Buffer())))
		rows = append(rows, Row{Entry: entry, Offset: offset})
	}
}