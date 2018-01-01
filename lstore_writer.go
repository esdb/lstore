package lstore

import (
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
	"github.com/v2pro/plz"
)

type writerCommand func(ctx countlog.Context)

type writer struct {
	store          *Store
	commandQueue   chan writerCommand
	currentVersion *StoreVersion
	tailSeq        uint64 // relative seq to file header
	tailRows       rowsChunk
	writeBuf       []byte
	writeMMap      mmap.MMap
}

type WriteResult struct {
	Offset Offset
	Error  error
}

func (store *Store) newWriter(ctx countlog.Context) (*writer, error) {
	writer := &writer{
		store:        store,
		commandQueue: make(chan writerCommand, store.Config.CommandQueueSize),
	}
	if err := writer.load(ctx); err != nil {
		return nil, err
	}
	writer.start()
	return writer, nil
}

func (writer *writer) Close() error {
	return plz.Close(plz.WrapCloser(writer.writeMMap.Unmap))
}

func (writer *writer) load(ctx countlog.Context) error {
	store := writer.store
	config := store.Config
	initialVersion, err := loadInitialVersion(ctx, &config)
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
	reader, err := store.NewReader(context.Background())
	if err != nil {
		return err
	}
	defer reader.Close()
	writer.tailRows = reader.tailRows
	writer.tailSeq = reader.tailSeq
	tailSegment.updateTail(reader.tailOffset)
	return nil
}

func (writer *writer) start() {
	writer.store.executor.Go(func(ctxObj context.Context) {
		ctx := countlog.Ctx(ctxObj)
		defer func() {
			countlog.Info("event!writer.stop")
			err := writer.currentVersion.Close()
			if err != nil {
				countlog.Error("event!store.failed to close", "err", err)
			}
		}()
		countlog.Info("event!writer.start")
		stream := gocodec.NewStream(nil)
		ctx = countlog.Ctx(context.WithValue(ctx, "stream", stream))
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

func (writer *writer) asyncExecute(ctx countlog.Context, cmd writerCommand) {
	select {
	case writer.commandQueue <- cmd:
	case <-ctx.Done():
	}
}

func handleCommand(ctx countlog.Context, cmd writerCommand) {
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

func (writer *writer) AsyncWrite(ctxObj context.Context, entry *Entry, resultChan chan<- WriteResult) {
	ctx := countlog.Ctx(ctxObj)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
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

func (writer *writer) Write(ctxObj context.Context, entry *Entry) (Offset, error) {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan WriteResult)
	writer.AsyncWrite(ctx, entry, resultChan)
	select {
	case result := <-resultChan:
		ctx.DebugCall("callee!writer.AsyncWrite", result.Error, "offset", result.Offset,
			"tail", func() interface{} {
				return writer.store.latest().tailSegment.getTail()
			})
		return result.Offset, result.Error
	case <-ctx.Done():
		ctx.DebugCall("callee!writer.AsyncWrite", ctx.Err())
		return 0, ctx.Err()
	}
}

func (writer *writer) tryWrite(ctx countlog.Context, tailSegment *TailSegment, entry *Entry) (Offset, error) {
	buf := writer.writeBuf
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(buf[writer.tailSeq:writer.tailSeq])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return 0, stream.Error
	}
	tail := writer.tailSeq + size
	if tail >= uint64(len(buf)) {
		return 0, SegmentOverflowError
	}
	offset := tailSegment.startOffset + Offset(len(writer.tailRows.rows))
	writer.tailRows.rows = append(writer.tailRows.rows, entry)
	writer.tailSeq += size
	countlog.Trace("event!writer.tryWrite", "tailSeq", writer.tailSeq, "offset", offset)
	// reader will know if read the tail using atomic
	tailSegment.updateTail(offset + 1)
	return offset, nil
}

func (writer *writer) rotate(oldVersion *StoreVersion) (*StoreVersion, error) {
	err := writer.writeMMap.Unmap()
	if err != nil {
		countlog.Error("event!writer.failed to unmap write", "err", err)
		return nil, err
	}
	newVersion := oldVersion.edit()
	newVersion.rawSegments = make([]*rawSegment, len(oldVersion.rawSegments)+1)
	i := 0
	for ; i < len(oldVersion.rawSegments); i++ {
		newVersion.rawSegments[i] = oldVersion.rawSegments[i]
	}
	conf := oldVersion.config
	rotatedTo := writer.store.RawSegmentPath(Offset(oldVersion.tailSegment.tail))
	if err = os.Rename(oldVersion.tailSegment.path, rotatedTo); err != nil {
		return nil, err
	}
	// use writer.tailRows to build a raw segment without loading from file
	newVersion.rawSegments[i] = &rawSegment{
		segmentHeader: oldVersion.tailSegment.segmentHeader,
		Path:          rotatedTo,
		rows:          writer.tailRows,
		ReferenceCounted: ref.NewReferenceCounted(fmt.Sprintf("raw segment@%d",
			oldVersion.tailSegment.segmentHeader.startOffset)),
	}
	writer.tailSeq = 0
	writer.tailRows = newRowsChunk(0)
	newVersion.tailSegment, err = openTailSegment(
		conf.TailSegmentPath(), conf.TailSegmentMaxSize, Offset(oldVersion.tailSegment.tail))
	if err != nil {
		return nil, err
	}
	err = writer.mapFile(newVersion.tailSegment)
	if err != nil {
		return nil, err
	}
	newVersion.tailSegment.updateTail(newVersion.tailSegment.startOffset)
	countlog.Debug("event!store.rotated",
		"tail", newVersion.tailSegment.startOffset,
		"rotatedTo", rotatedTo)
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

func (writer *writer) purgeRawSegments(
	ctx countlog.Context, purgedRawSegmentsCount int) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		newVersion.rawSegments = oldVersion.rawSegments[purgedRawSegmentsCount:]
		writer.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.purged raw segments",
		"count", purgedRawSegmentsCount)
	return <-resultChan
}
