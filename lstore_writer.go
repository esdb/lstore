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
	"github.com/esdb/lstore/ref"
	"io"
	"math"
)

type command func(ctx context.Context, store *StoreVersion) *StoreVersion

type writer struct {
	store          *Store
	commandQueue   chan command
	currentVersion *StoreVersion
	tailRows       []Row
	writeBuf       []byte
	writeMMap      mmap.MMap
}

type WriteResult struct {
	Seq   RowSeq
	Error error
}

func (store *Store) newWriter() (*writer, error) {
	writer := &writer{
		store:        store,
		commandQueue: make(chan command, store.Config.CommandQueueSize),
	}
	if err := writer.load(); err != nil {
		return nil, err
	}
	writer.start()
	return writer, nil
}

func (writer *writer) load() error {
	store := writer.store
	config := store.Config
	initialVersion, err := loadInitialVersion(config)
	if err != nil {
		return err
	}
	atomic.StorePointer(&store.currentVersion, unsafe.Pointer(initialVersion))
	writer.currentVersion = initialVersion
	tailSegment := initialVersion.tailSegment
	err = writer.mapFile(tailSegment)
	if err != nil {
		return err
	}
	// force reader to read all remaining rows
	tailSegment.updateTail(math.MaxUint64)
	reader, err := store.NewReader()
	if err != nil {
		return err
	}
	defer reader.Close()
	writer.tailRows = reader.tailRows.rows
	tailSegment.updateTail(reader.tailSeq)
	writer.start()
	return nil
}

func loadInitialVersion(config Config) (*StoreVersion, error) {
	tailSegment, err := openTailSegment(config.TailSegmentPath(), config.TailSegmentMaxSize, 0)
	if err != nil {
		return nil, err
	}
	resources := []io.Closer{tailSegment}
	var reversedRawSegments []*RawSegment
	startSeq := tailSegment.StartSeq
	for startSeq != 0 {
		prev := path.Join(config.Directory, fmt.Sprintf("%d.segment", startSeq))
		rawSegment, err := openRawSegment(prev)
		if err != nil {
			return nil, err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		resources = append(resources, rawSegment)
		startSeq = rawSegment.StartSeq
	}
	rawSegments := make([]*RawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	initialVersion := &StoreVersion{
		ReferenceCounted: ref.NewReferenceCounted("store version", resources...),
		config:           config,
		rawSegments:      rawSegments,
		tailSegment:      tailSegment,
	}
	return initialVersion, nil
}

func (writer *writer) start() {
	writer.store.executor.Go(func(ctx context.Context) {
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
				atomic.StorePointer(&writer.store.currentVersion, unsafe.Pointer(newVersion))
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
		seq, err := writer.tryWrite(ctx, writer.currentVersion.tailSegment, entry)
		if err == SegmentOverflowError {
			newVersion, err := writer.addSegment(currentVersion)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return newVersion
			}
			seq, err = writer.tryWrite(ctx, newVersion.tailSegment, entry)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return newVersion
			}
			resultChan <- WriteResult{seq, nil}
			return newVersion
		}
		resultChan <- WriteResult{seq, nil}
		return nil
	})
}

func (writer *writer) Write(ctx context.Context, entry *Entry) (RowSeq, error) {
	resultChan := make(chan WriteResult)
	writer.AsyncWrite(ctx, entry, resultChan)
	select {
	case result := <-resultChan:
		return result.Seq, result.Error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (writer *writer) tryWrite(ctx context.Context, tailSegment *TailSegment, entry *Entry) (RowSeq, error) {
	buf := writer.writeBuf
	maxTail := tailSegment.StartSeq + RowSeq(len(buf))
	seq := RowSeq(tailSegment.tail)
	if seq >= maxTail {
		return 0, SegmentOverflowError
	}
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(buf[seq-tailSegment.StartSeq:seq-tailSegment.StartSeq])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return 0, stream.Error
	}
	tail := seq + RowSeq(size)
	if tail >= maxTail {
		return 0, SegmentOverflowError
	}
	writer.tailRows = append(writer.tailRows, Row{Seq: seq, Entry: entry})
	// reader will know if read the tail using atomic
	tailSegment.updateTail(tail)
	return seq, nil
}

func (writer *writer) addSegment(oldVersion *StoreVersion) (*StoreVersion, error) {
	err := writer.writeMMap.Unmap()
	if err != nil {
		countlog.Error("event!writer.failed to unmap write", "err", err)
		return nil, err
	}
	newVersion := &StoreVersion{config: oldVersion.config}
	newVersion.rawSegments = make([]*RawSegment, len(oldVersion.rawSegments)+1)
	i := 0
	var resources []io.Closer
	for ; i < len(oldVersion.rawSegments); i++ {
		oldSegment := oldVersion.rawSegments[i]
		oldSegment.Acquire()
		newVersion.rawSegments[i] = oldSegment
		resources = append(resources, oldSegment)
	}
	conf := oldVersion.config
	rotatedTo := path.Join(conf.Directory, fmt.Sprintf("%d.segment", oldVersion.tailSegment.tail))
	if err = os.Rename(oldVersion.tailSegment.Path, rotatedTo); err != nil {
		return nil, err
	}
	// use writer.tailRows to build a raw segment without loading from file
	newVersion.rawSegments[i] = &RawSegment{
		SegmentHeader: oldVersion.tailSegment.SegmentHeader,
		Path:          rotatedTo,
		rows:       &rowsSegment{writer.tailRows},
		ReferenceCounted: ref.NewReferenceCounted(fmt.Sprintf("raw segment@%d",
			oldVersion.tailSegment.SegmentHeader.StartSeq)),
	}
	writer.tailRows = nil
	newVersion.tailSegment, err = openTailSegment(
		conf.TailSegmentPath(), conf.TailSegmentMaxSize, RowSeq(oldVersion.tailSegment.tail))
	if err != nil {
		return nil, err
	}
	resources = append(resources, newVersion.tailSegment)
	err = writer.mapFile(newVersion.tailSegment)
	if err != nil {
		return nil, err
	}
	newVersion.tailSegment.updateTail(newVersion.tailSegment.StartSeq)
	newVersion.ReferenceCounted = ref.NewReferenceCounted("store version", resources...)
	countlog.Info("event!store.rotated", "tail", newVersion.tailSegment.StartSeq)
	return newVersion, nil
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
