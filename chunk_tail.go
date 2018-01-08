package lstore

import (
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"errors"
	"github.com/v2pro/plz"
	"github.com/esdb/gocodec"
	"fmt"
	"io"
	"os"
)

var SegmentOverflowError = errors.New("please rotate to new chunk")

// tailChunk is owned by writer
type tailChunk struct {
	*segmentHeader
	tailEntriesCount int
	writeBuf         []byte
	writeMMap        mmap.MMap
}

func (chunk *tailChunk) Close() error {
	if chunk.writeMMap == nil {
		return nil
	}
	err := chunk.writeMMap.Unmap()
	countlog.TraceCall("callee!writeMMap.Unmap", err)
	return err
}

func openTailChunk(ctx countlog.Context, path string, maxSize int64, headOffset Offset) (*tailChunk, []*Entry, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = createRawSegment(path, maxSize, headOffset)
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
	chunk := &tailChunk{writeMMap: writeMMap}
	iter := gocodec.NewIterator(writeMMap)
	segmentHeader, _ := iter.Copy((*segmentHeader)(nil)).(*segmentHeader)
	ctx.TraceCall("callee!iter.Copy", iter.Error)
	if iter.Error != nil {
		plz.Close(plz.WrapCloser(writeMMap.Unmap))
		return nil, nil, fmt.Errorf("openTailChunk: %s", iter.Error.Error())
	}
	var entries []*Entry
	for {
		entry, _ := iter.Copy((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			break
		}
		ctx.TraceCall("callee!iter.Copy", iter.Error)
		if iter.Error != nil {
			plz.Close(plz.WrapCloser(writeMMap.Unmap))
			return nil, nil, fmt.Errorf("openTailChunk: %s", iter.Error.Error())
		}
		entries = append(entries, entry)
	}
	chunk.segmentHeader = segmentHeader
	chunk.writeBuf = iter.Buffer()
	chunk.tailEntriesCount = len(entries)
	return chunk, entries, nil
}
