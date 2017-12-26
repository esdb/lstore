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
	"math"
	"github.com/esdb/lstore/ref"
)

type command func(ctx context.Context, store *StoreVersion) *StoreVersion

type writer struct {
	commandQueue   chan command
	currentVersion *StoreVersion
	tailRows       []Row
	writeBuf       []byte
	writeMMap      mmap.MMap
}

type WriteResult struct {
	Offset Offset
	Error  error
}

func loadWriter(store *Store) (*writer, error) {
	config := store.Config
	initialVersion, err := loadInitialVersion(config)
	if err != nil {
		return nil, err
	}
	atomic.StorePointer(&store.currentVersion, unsafe.Pointer(initialVersion))
	writer := &writer{
		commandQueue:   make(chan command, config.CommandQueueSize),
		currentVersion: initialVersion,
	}
	tailSegment := initialVersion.tailSegment
	err = writer.mapFile(tailSegment)
	if err != nil {
		return nil, err
	}
	// force reader to read all remaining rows
	tailSegment.updateTail(math.MaxUint64)
	reader, err := store.NewReader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	writer.tailRows = reader.tailRows
	tailSegment.updateTail(reader.tailOffset)
	writer.start(store)
	return writer, nil
}

func loadInitialVersion(config Config) (*StoreVersion, error) {
	tailSegment, err := openTailSegment(config.TailSegmentPath(), config.TailSegmentMaxSize, 0)
	if err != nil {
		return nil, err
	}
	var reversedRawSegments []*RawSegment
	startOffset := tailSegment.StartOffset
	for startOffset != 0 {
		prev := path.Join(config.Directory, fmt.Sprintf("%d.segment", startOffset))
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
	initialVersion := &StoreVersion{
		refCnt:      ref.NewReferenceCounted("store version", tailSegment),
		config:      config,
		rawSegments: rawSegments,
		tailSegment: tailSegment,
	}
	return initialVersion, nil
}

func (writer *writer) start(store *Store) {
	store.executor.Go(func(ctx context.Context) {
		defer func() {
			err := writer.currentVersion.Close()
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
			newVersion := handleCommand(ctx, cmd, writer.currentVersion)
			if newVersion != nil {
				atomic.StorePointer(&store.currentVersion, unsafe.Pointer(newVersion))
				err := writer.currentVersion.Close()
				if err != nil {
					countlog.Error("event!store.failed to close", "err", err)
				}
				writer.currentVersion = newVersion
			}
		}
	})
}

func (writer *writer) asyncExecute(ctx context.Context, cmd command) {
	select {
	case writer.commandQueue <- cmd:
	case <-ctx.Done():
	}
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

func (writer *writer) AsyncWrite(ctx context.Context, entry *Entry, resultChan chan<- WriteResult) {
	writer.asyncExecute(ctx, func(ctx context.Context, currentVersion *StoreVersion) *StoreVersion {
		offset, err := writer.tryWrite(ctx, writer.currentVersion.tailSegment, entry)
		if err == SegmentOverflowError {
			newVersion, err := writer.addSegment(currentVersion)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return newVersion
			}
			offset, err = writer.tryWrite(ctx, newVersion.tailSegment, entry)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return newVersion
			}
			resultChan <- WriteResult{offset, nil}
			return newVersion
		}
		resultChan <- WriteResult{offset, nil}
		return nil
	})
}

func (writer *writer) Write(ctx context.Context, entry *Entry) (Offset, error) {
	resultChan := make(chan WriteResult)
	writer.AsyncWrite(ctx, entry, resultChan)
	select {
	case result := <-resultChan:
		return result.Offset, result.Error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (writer *writer) tryWrite(ctx context.Context, tailSegment *TailSegment, entry *Entry) (Offset, error) {
	buf := writer.writeBuf
	maxTail := tailSegment.StartOffset + Offset(len(buf))
	offset := Offset(tailSegment.tail)
	if offset >= maxTail {
		return 0, SegmentOverflowError
	}
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(buf[offset-tailSegment.StartOffset:offset-tailSegment.StartOffset])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return 0, stream.Error
	}
	tail := offset + Offset(size)
	if tail >= maxTail {
		return 0, SegmentOverflowError
	}
	// reader will know if read the tail using atomic
	tailSegment.updateTail(tail)
	return offset, nil
}

func (writer *writer) addSegment(oldVersion *StoreVersion) (*StoreVersion, error) {
	err := writer.writeMMap.Unmap()
	if err != nil {
		countlog.Error("event!writer.failed to unmap write", "err", err)
		return nil, err
	}
	newVersion := *oldVersion
	newVersion.rawSegments = make([]*RawSegment, len(oldVersion.rawSegments)+1)
	i := 0
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
	err = writer.mapFile(newVersion.tailSegment)
	if err != nil {
		return nil, err
	}
	newVersion.tailSegment.updateTail(newVersion.tailSegment.StartOffset)
	newVersion.refCnt = ref.NewReferenceCounted("store version", newVersion.tailSegment)
	countlog.Info("event!store.rotated", "tail", newVersion.tailSegment.StartOffset)
	return &newVersion, nil
}

func (writer *writer) mapFile(tailSegment *TailSegment) error {
	writeMMap, err := mmap.Map(tailSegment.file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!segment.failed to mmap as RDWR", "err", err, "path", tailSegment.Path)
		return err
	}
	writer.writeMMap = writeMMap
	iter := gocodec.NewIterator(writeMMap)
	iter.Skip()
	writer.writeBuf = iter.Buffer()
	return nil
}
