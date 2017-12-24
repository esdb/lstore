package lstore

import (
	"path"
	"fmt"
	"github.com/v2pro/plz/countlog"
	"context"
	"os"
	"github.com/v2pro/plz/concurrent"
	"unsafe"
	"sync/atomic"
	"github.com/esdb/gocodec"
	"github.com/edsrzf/mmap-go"
)

type command func(ctx context.Context, store *StoreVersion) *StoreVersion
type Writer struct {
	commandQueue chan command
	tailSegment  *TailSegment
	writeBuf     []byte
	writeMMap    mmap.MMap
}

func (writer *Writer) asyncExecute(ctx context.Context, cmd command) {
	select {
	case writer.commandQueue <- cmd:
	case <-ctx.Done():
	}
}

func (writer *Writer) start(store *Store, initialVersion *StoreVersion) {
	store.executor.Go(func(ctx context.Context) {
		currentVersion := initialVersion
		defer func() {
			err := currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		stream := gocodec.NewStream(nil)
		ctx = context.WithValue(ctx, "stream", stream)
		for {
			var cmd command
			select {
			case <-ctx.Done():
				return
			case cmd = <-writer.commandQueue:
			}
			newVersion := handleCommand(ctx, cmd, currentVersion)
			if newVersion != nil {
				atomic.StorePointer(&store.currentVersion, unsafe.Pointer(newVersion))
				err := currentVersion.Close()
				if err != nil {
					countlog.Error("event!store.failed to close", "err", err)
				}
				currentVersion = newVersion
			}
		}
	})
}

func (store *Store) loadData() (*StoreVersion, error) {
	tailSegment, err := openTailSegment(store.TailSegmentPath(), store.TailSegmentMaxSize, 0)
	if err != nil {
		return nil, err
	}
	var reversedRawSegments []*RawSegment
	startOffset := tailSegment.StartOffset
	for startOffset != 0 {
		prev := path.Join(store.Directory, fmt.Sprintf("%d.segment", startOffset))
		rawSegment, err := openRawSegment(prev, startOffset)
		if err != nil {
			return nil, err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		startOffset = rawSegment.StartOffset
	}
	rawSegments := make([]*RawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	return &StoreVersion{
		referenceCounter: 1,
		config:           store.Config,
		rawSegments:      rawSegments,
		tailSegment:      tailSegment,
	}, nil
}

func handleCommand(ctx context.Context, cmd command, currentVersion *StoreVersion) *StoreVersion {
	defer func() {
		recovered := recover()
		if recovered != nil && recovered != concurrent.StopSignal {
			countlog.Fatal("event!store.panic",
				"err", recovered,
				"stacktrace", countlog.ProvideStacktrace)
		}
	}()
	return cmd(ctx, currentVersion)
}

type WriteResult struct {
	Offset Offset
	Error  error
}

func (store *Store) AsyncWrite(ctx context.Context, entry *Entry, resultChan chan<- WriteResult) {
	store.asyncExecute(ctx, func(ctx context.Context, currentVersion *StoreVersion) *StoreVersion {
		offset, err := store.Write(ctx, entry)
		if err == SegmentOverflowError {
			newVersion, err := store.addSegment(currentVersion)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return nil
			}
			offset, err = store.Write(ctx, entry)
			resultChan <- WriteResult{offset, nil}
			return newVersion
		}
		resultChan <- WriteResult{offset, nil}
		return nil
	})
}

func (store *Store) Write(ctx context.Context, entry *Entry) (Offset, error) {
	resultChan := make(chan WriteResult)
	store.AsyncWrite(ctx, entry, resultChan)
	select {
	case result := <-resultChan:
		return result.Offset, result.Error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (writer *Writer) write(ctx context.Context, entry *Entry) (Offset, error) {
	buf := writer.writeBuf
	segment := writer.tailSegment
	maxTail := segment.StartOffset + Offset(len(buf))
	offset := Offset(segment.tail)
	if offset >= maxTail {
		return 0, SegmentOverflowError
	}
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(buf[offset-segment.StartOffset:offset-segment.StartOffset])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return 0, stream.Error
	}
	tail := offset + Offset(size)
	if tail >= maxTail {
		return 0, SegmentOverflowError
	}
	segment.updateTail(tail)
	return offset, nil
}

func (writer *Writer) addSegment(oldVersion *StoreVersion) (*StoreVersion, error) {
	newVersion := *oldVersion
	newVersion.referenceCounter = 1
	newVersion.rawSegments = make([]*RawSegment, len(oldVersion.rawSegments)+1)
	i := 0
	var err error
	for ; i < len(oldVersion.rawSegments); i++ {
		oldSegment := oldVersion.rawSegments[i]
		newVersion.rawSegments[i] = oldSegment
		if err != nil {
			return nil, err
		}
	}
	conf := oldVersion.config
	rotatedTo := path.Join(conf.Directory, fmt.Sprintf("%d.segment", oldVersion.tailSegment.tail))
	if err = os.Rename(oldVersion.tailSegment.Path, rotatedTo); err != nil {
		return nil, err
	}
	newVersion.rawSegments[i], err = openRawSegment(rotatedTo, Offset(oldVersion.tailSegment.tail))
	newVersion.tailSegment, err = openTailSegment(
		conf.TailSegmentPath(), conf.TailSegmentMaxSize, Offset(oldVersion.tailSegment.tail))
	if err != nil {
		return nil, err
	}
	countlog.Info("event!store.rotated", "tail", newVersion.tailSegment.StartOffset)
	return &newVersion, nil
}
