package lstore

import (
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/gocodec"
	"errors"
	"io"
	"github.com/esdb/lstore/ref"
	"path"
	"github.com/v2pro/plz"
)

var SegmentOverflowError = errors.New("please rotate to new chunk")

// writableTailSegment is owned by writer
type tailSegment struct {
	*rawSegment
	writeBuf  []byte
	writeMMap mmap.MMap
}

func (segment *tailSegment) Close() error {
	if segment.writeMMap == nil {
		return nil
	}
	err := segment.writeMMap.Unmap()
	countlog.TraceCall("callee!writeMMap.Unmap", err)
	return err
}

func openTailSegment(ctx countlog.Context, path string, maxSize int64, startOffset Offset) (*tailSegment, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = createTailSegment(path, maxSize, startOffset)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	defer plz.Close(file)
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	ctx.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return nil, err
	}
	segment := &tailSegment{}
	iter := gocodec.NewIterator(writeMMap)
	segmentHeader, _ := iter.Copy((*segmentHeader)(nil)).(*segmentHeader)
	if iter.Error != nil {
		plz.Close(plz.WrapCloser(writeMMap.Unmap))
		return nil, iter.Error
	}
	segment.rawSegment = &rawSegment{
		segmentHeader: *segmentHeader,
		ReferenceCounted: ref.NewReferenceCounted("in mem raw segment"),
	}
	for {
		entry, _ := iter.Copy((*Entry)(nil)).(*Entry)
		if iter.Error == io.EOF {
			break
		}
		if iter.Error != nil {
			plz.Close(plz.WrapCloser(writeMMap.Unmap))
			return nil, iter.Error
		}
		segment.entries = append(segment.entries, entry)
	}
	segment.writeBuf = iter.Buffer()
	return segment, nil
}

func createTailSegment(filename string, maxSize int64, startOffset Offset) (*os.File, error) {
	os.MkdirAll(path.Dir(filename), 0777)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	err = file.Truncate(maxSize)
	if err != nil {
		return nil, err
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(segmentHeader{segmentType: segmentTypeTail, startOffset: startOffset})
	if stream.Error != nil {
		return nil, stream.Error
	}
	_, err = file.Write(stream.Buffer())
	if err != nil {
		return nil, err
	}
	return os.OpenFile(filename, os.O_RDWR, 0666)
}
