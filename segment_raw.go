package lstore

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"os"
	"io"
	"github.com/esdb/lstore/ref"
	"fmt"
	"github.com/v2pro/plz"
	"strconv"
)

type rawSegment struct {
	segmentHeader
	*ref.ReferenceCounted
	rows []*Entry
}

func openRawSegment(ctx countlog.Context, path string) (*rawSegment, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	ctx.TraceCall("callee!os.OpenFile", err)
	if err != nil {
		return nil, err
	}
	segment := &rawSegment{}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		plz.Close(file)
		countlog.Error("event!raw.failed to mmap as COPY", "err", err, "path", path)
		return nil, err
	}
	plz.Close(file)
	var resources []io.Closer
	resources = append(resources, plz.WrapCloser(readMMap.Unmap))
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.Unmarshal((*segmentHeader)(nil)).(*segmentHeader)
	if iter.Error != nil {
		countlog.Error("event!raw.failed to unmarshal header", "err", iter.Error, "path", path)
		return nil, iter.Error
	}
	segment.segmentHeader = *segmentHeader
	segment.rows, err = segment.loadRows(ctx, iter)
	if err != nil {
		countlog.Error("event!raw.failed to unmarshal rows", "err", iter.Error, "path", path)
		return nil, err
	}
	segment.ReferenceCounted = ref.NewReferenceCounted(fmt.Sprintf("raw segment@%d", segment.startOffset), resources...)
	return segment, nil
}

func (segment *rawSegment) loadRows(ctx countlog.Context, iter *gocodec.Iterator) ([]*Entry, error) {
	var rows []*Entry
	for {
		iter.Reset(iter.Buffer())
		entry, _ := iter.Unmarshal((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			return rows, nil
		}
		ctx.TraceCall("callee!iter.Unmarshal", iter.Error)
		if iter.Error != nil {
			return nil, fmt.Errorf("load raw segment rows failed: %v", iter.Error.Error())
		}
		rows = append(rows, entry)
	}
}

func (segment *rawSegment) search(ctx countlog.Context, startOffset Offset, tailOffset Offset,
	filter Filter, cb SearchCallback) error {
	for i, entry := range segment.rows {
		offset := segment.startOffset + Offset(i)
		if offset < startOffset || offset >= tailOffset {
			continue
		}
		if filter.matchesEntry(entry) {
			if err := cb.HandleRow(offset, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func (segment *rawSegment) String() string {
	if len(segment.rows) == 0 {
		return "rawSegment{}"
	}
	start := int(segment.startOffset)
	end := start + len(segment.rows) - 1
	desc := "rawSegment{" + strconv.Itoa(start) + "~" + strconv.Itoa(end) + "}"
	return desc
}
