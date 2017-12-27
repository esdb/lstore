package lstore

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"os"
	"io"
	"github.com/esdb/lstore/ref"
	"fmt"
)

type RawSegment struct {
	SegmentHeader
	*ref.ReferenceCounted
	rows segment
	Path string
}

func openRawSegment(path string) (*RawSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}
	var resources []io.Closer
	resources = append(resources, file)
	segment := &RawSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as COPY", "err", err, "path", path)
		return nil, err
	}
	resources = append(resources, ref.NewResource("mmap as COPY", func() error {
		return readMMap.Unmap()
	}))
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Unmarshal((*SegmentHeader)(nil)).(*SegmentHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	segment.SegmentHeader = *segmentHeader
	segment.Path = path
	segment.rows, err = segment.loadRows(iter)
	if err != nil {
		return nil, err
	}
	segment.ReferenceCounted = ref.NewReferenceCounted(fmt.Sprintf("raw segment@%d", segment.StartSeq), resources...)
	return segment, nil
}

func (segment *RawSegment) loadRows(iter *gocodec.Iterator) (*rowsSegment, error) {
	var rows []Row
	startSeq := segment.StartSeq
	totalSize := RowSeq(len(iter.Buffer()))
	for {
		entry, _ := iter.Unmarshal((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			return &rowsSegment{rows}, nil
		}
		if iter.Error != nil {
			return nil, iter.Error
		}
		seq := startSeq + (totalSize - RowSeq(len(iter.Buffer())))
		rows = append(rows, Row{Entry: entry, Seq: seq})
	}
}

func (segment *RawSegment) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	return segment.rows.search(reader, startSeq, filters, collector)
}
