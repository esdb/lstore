package lstore

import (
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/v2pro/plz"
	"github.com/v2pro/plz/countlog"
	"io"
	"os"
)

var SegmentOverflowError = errors.New("please rotate to new chunk")

// tailSegment is owned by appender
type tailSegment struct {
	*segmentHeader
	tailEntriesCount int
	writeBuf         []byte
	writeMMap        mmap.MMap
}

func (chunk *tailSegment) Close() error {
	if chunk.writeMMap == nil {
		return nil
	}
	err := chunk.writeMMap.Unmap()
	countlog.TraceCall("callee!writeMMap.Unmap", err)
	return err
}

func openTailSegment(ctx *countlog.Context, path string, maxSize int64, headOffset Offset) (*tailSegment, []*Entry, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = createRawSegment(ctx, path, maxSize, headOffset)
		if err != nil {
			return nil, nil, err
		}
	} else if err != nil {
		ctx.TraceCall("callee!os.OpenFile", err)
		return nil, nil, err
	}
	defer plz.Close(file)
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	ctx.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return nil, nil, err
	}
	segment := &tailSegment{writeMMap: writeMMap}
	iter := gocodec.NewIterator(writeMMap)
	segmentHeader, _ := iter.CopyThenUnmarshal((*segmentHeader)(nil)).(*segmentHeader)
	ctx.TraceCall("callee!iter.Copy", iter.Error)
	if iter.Error != nil {
		plz.Close(plz.WrapCloser(writeMMap.Unmap))
		return nil, nil, fmt.Errorf("openTailSegment: %s", iter.Error.Error())
	}
	var entries []*Entry
	for {
		entry, _ := iter.CopyThenUnmarshal((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			break
		}
		ctx.TraceCall("callee!iter.Copy", iter.Error)
		if iter.Error != nil {
			plz.Close(plz.WrapCloser(writeMMap.Unmap))
			return nil, nil, fmt.Errorf("openTailSegment: %s", iter.Error.Error())
		}
		entries = append(entries, entry)
	}
	segment.segmentHeader = segmentHeader
	segment.writeBuf = iter.Buffer()
	segment.tailEntriesCount = len(entries)
	return segment, entries, nil
}
