package lstore

import (
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/v2pro/plz"
	"github.com/v2pro/plz/countlog"
	"io"
	"os"
	"path"
)

type rawSegment struct {
	segmentHeader
	path string
}

func openRawSegment(ctx *countlog.Context, path string) (*rawSegment, []*Entry, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	ctx.TraceCall("callee!os.OpenFile", err)
	if err != nil {
		return nil, nil, err
	}
	defer plz.Close(file)
	segment := &rawSegment{path: path}
	readMMap, err := mmap.Map(file, mmap.RDONLY, 0)
	ctx.TraceCall("callee!mmap.Map", err, "path", path)
	if err != nil {
		return nil, nil, fmt.Errorf("openRawSegment: %s", err.Error())
	}
	defer plz.Close(plz.WrapCloser(readMMap.Unmap))
	iter := gocodec.NewIterator(readMMap)
	segmentHeader, _ := iter.CopyThenUnmarshal((*segmentHeader)(nil)).(*segmentHeader)
	ctx.TraceCall("callee!iter.Copy", iter.Error)
	if iter.Error != nil {
		return nil, nil, fmt.Errorf("openRawSegment: %s", iter.Error.Error())
	}
	segment.segmentHeader = *segmentHeader
	entries, err := segment.loadRows(ctx, iter)
	if err != nil {
		return nil, nil, err
	}
	return segment, entries, nil
}

func (segment *rawSegment) loadRows(ctx *countlog.Context, iter *gocodec.Iterator) ([]*Entry, error) {
	var rows []*Entry
	for {
		iter.Reset(iter.Buffer())
		entry, _ := iter.CopyThenUnmarshal((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			return rows, nil
		}
		ctx.TraceCall("callee!iter.Unmarshal", iter.Error)
		if iter.Error != nil {
			return nil, fmt.Errorf("rawSegment.loadRows: %v", iter.Error.Error())
		}
		rows = append(rows, entry)
	}
}

func createRawSegment(ctx *countlog.Context, segmentPath string, maxSize int64, headOffset Offset) (*os.File, error) {
	os.MkdirAll(path.Dir(segmentPath), 0777)
	file, err := os.OpenFile(segmentPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	err = file.Truncate(maxSize)
	if err != nil {
		return nil, err
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(segmentHeader{segmentType: segmentTypeRaw, headOffset: headOffset})
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
	ctx.DebugCall("callee!createRawSegment", err,
		"segmentPath", segmentPath, "headOffset", headOffset)
	if err != nil {
		return nil, err
	}
	return os.OpenFile(segmentPath, os.O_RDWR, 0666)
}
