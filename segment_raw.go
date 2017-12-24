package lstore

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"os"
	"io"
)

type RawSegment struct {
	SegmentHeader
	AsBlock   Block
	Path      string
}

func openRawSegment(path string, endOffset Offset) (*RawSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	segment := &RawSegment{}
	defer file.Close()
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as COPY", "err", err, "path", path)
		return nil, err
	}
	defer readMMap.Unmap()
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Copy((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	totalSize := endOffset - segment.StartOffset
	copied := append([]byte(nil), iter.Buffer()[:totalSize]...)
	iter.Reset(copied)
	segment.Path = path
	segment.AsBlock, err = segment.loadBlock(iter)
	if err != nil {
		return nil, err
	}
	return segment, nil
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