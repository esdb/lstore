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
	"math"
)

type writerCommand func(ctx context.Context)

type writer struct {
	store          *Store
	commandQueue   chan writerCommand
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
		commandQueue: make(chan writerCommand, store.Config.CommandQueueSize),
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
	initialVersion, err := loadInitialVersion(&config)
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
	writer.tailRows = reader.tailRows
	tailSegment.updateTail(reader.tailSeq)
	return nil
}

func (writer *writer) start() {
	writer.store.executor.Go(func(ctx context.Context) {
		defer func() {
			countlog.Info("event!writer.stop")
			err := writer.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		countlog.Info("event!writer.start")
		stream := gocodec.NewStream(nil)
		ctx = context.WithValue(ctx, "stream", stream)
		for {
			var cmd writerCommand
			select {
			case <-ctx.Done():
				return
			case cmd = <-writer.commandQueue:
			}
			handleCommand(ctx, cmd)
		}
	})
}

func (writer *writer) updateCurrentVersion(newVersion *StoreVersion) {
	if newVersion == nil {
		return
	}
	atomic.StorePointer(&writer.store.currentVersion, unsafe.Pointer(newVersion))
	err := writer.currentVersion.Close()
	if err != nil {
		countlog.Error("event!store.failed to close", "err", err)
	}
	writer.currentVersion = newVersion
}

func (writer *writer) asyncExecute(ctx context.Context, cmd writerCommand) {
	select {
	case writer.commandQueue <- cmd:
	case <-ctx.Done():
	}
}

func handleCommand(ctx context.Context, cmd writerCommand) {
	defer func() {
		recovered := recover()
		if recovered != nil && recovered != concurrent.StopSignal {
			countlog.Fatal("event!store.panic",
				"err", recovered,
				"stacktrace", countlog.ProvideStacktrace)
		}
	}()
	cmd(ctx)
}

func (writer *writer) AsyncWrite(ctx context.Context, entry *Entry, resultChan chan<- WriteResult) {
	writer.asyncExecute(ctx, func(ctx context.Context) {
		seq, err := writer.tryWrite(ctx, writer.currentVersion.tailSegment, entry)
		if err == SegmentOverflowError {
			newVersion, err := writer.rotate(writer.currentVersion)
			if err != nil {
				writer.updateCurrentVersion(newVersion)
				resultChan <- WriteResult{0, err}
				return
			}
			seq, err = writer.tryWrite(ctx, newVersion.tailSegment, entry)
			if err != nil {
				writer.updateCurrentVersion(newVersion)
				resultChan <- WriteResult{0, err}
				return
			}
			writer.updateCurrentVersion(newVersion)
			resultChan <- WriteResult{seq, nil}
			return
		}
		resultChan <- WriteResult{seq, nil}
		return
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
	maxTail := tailSegment.startSeq + RowSeq(len(buf))
	seq := RowSeq(tailSegment.tail)
	if seq >= maxTail {
		return 0, SegmentOverflowError
	}
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(buf[seq-tailSegment.startSeq:seq-tailSegment.startSeq])
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

func (writer *writer) rotate(oldVersion *StoreVersion) (*StoreVersion, error) {
	err := writer.writeMMap.Unmap()
	if err != nil {
		countlog.Error("event!writer.failed to unmap write", "err", err)
		return nil, err
	}
	newVersion := oldVersion.edit()
	newVersion.rawSegments = make([]*RawSegment, len(oldVersion.rawSegments)+1)
	i := 0
	for ; i < len(oldVersion.rawSegments); i++ {
		newVersion.rawSegments[i] = oldVersion.rawSegments[i]
	}
	conf := oldVersion.config
	rotatedTo := path.Join(conf.Directory, fmt.Sprintf("%d.segment", oldVersion.tailSegment.tail))
	if err = os.Rename(oldVersion.tailSegment.path, rotatedTo); err != nil {
		return nil, err
	}
	// use writer.tailRows to build a raw segment without loading from file
	newVersion.rawSegments[i] = &RawSegment{
		segmentHeader: oldVersion.tailSegment.segmentHeader,
		Path:          rotatedTo,
		rows:          writer.tailRows,
		ReferenceCounted: ref.NewReferenceCounted(fmt.Sprintf("raw segment@%d",
			oldVersion.tailSegment.segmentHeader.startSeq)),
	}
	writer.tailRows = nil
	newVersion.tailSegment, err = openTailSegment(
		conf.TailSegmentPath(), conf.TailSegmentMaxSize, RowSeq(oldVersion.tailSegment.tail))
	if err != nil {
		return nil, err
	}
	err = writer.mapFile(newVersion.tailSegment)
	if err != nil {
		return nil, err
	}
	newVersion.tailSegment.updateTail(newVersion.tailSegment.startSeq)
	countlog.Info("event!store.rotated", "tail", newVersion.tailSegment.startSeq)
	return newVersion.seal(), nil
}

func (writer *writer) mapFile(tailSegment *TailSegment) error {
	writeMMap, err := mmap.Map(tailSegment.file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!chunk.failed to mmap as RDWR", "err", err, "path", tailSegment.path)
		return err
	}
	writer.writeMMap = writeMMap
	iter := gocodec.NewIterator(writeMMap)
	iter.Skip()
	writer.writeBuf = iter.Buffer()
	return nil
}
