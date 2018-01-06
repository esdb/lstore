package lstore

import (
	"github.com/v2pro/plz/countlog"
	"context"
	"os"
	"github.com/v2pro/plz/concurrent"
	"unsafe"
	"sync/atomic"
	"github.com/esdb/gocodec"
	"github.com/v2pro/plz"
)

type writerCommand func(ctx countlog.Context)

type writer struct {
	store          *Store
	commandQueue   chan writerCommand
	currentVersion *StoreVersion
	*tailSegment
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
	return writer.tailSegment.Close()
}

func (writer *writer) load(ctx countlog.Context) error {
	err := writer.loadInitialVersion(ctx)
	if err != nil {
		return err
	}
	writer.updateTailOffset(writer.tailSegment.startOffset + Offset(len(writer.tailSegment.rows)))
	return nil
}

func (writer *writer) loadInitialVersion(ctx countlog.Context) error {
	version := StoreVersion{}.edit()
	err := writer.loadIndexingAndIndexedSegments(ctx, version)
	ctx.TraceCall("callee!writer.loadIndexingAndIndexedSegments", err)
	if err != nil {
		return err
	}
	err = writer.loadTailAndRawSegments(ctx, version)
	ctx.TraceCall("callee!writer.loadTailAndRawSegments", err)
	if err != nil {
		return err
	}
	writer.updateCurrentVersion(version.seal())
	return nil
}

func (writer *writer) loadIndexingAndIndexedSegments(ctx countlog.Context, version *EditingStoreVersion) error {
	indexingSegment, err := openIndexingSegment(
		ctx, writer.store.IndexingSegmentPath(), nil,
		writer.store.blockManager, writer.store.slotIndexManager)
	ctx.TraceCall("callee!store.openIndexingSegment", err)
	if err != nil {
		return err
	}
	startOffset := indexingSegment.startOffset
	var reversedIndexedSegments []*searchable
	for {
		indexedSegmentPath := writer.store.IndexedSegmentPath(startOffset)
		indexedSegment, err := openIndexedSegment(ctx, indexedSegmentPath)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		reversedIndexedSegments = append(reversedIndexedSegments, &searchable{
			indexSegment: indexedSegment,
			blockManager: writer.store.blockManager,
			readSlotIndex: writer.store.slotIndexManager.readSlotIndex,
		})
		startOffset = indexedSegment.startOffset
	}
	indexedSegments := make([]*searchable, len(reversedIndexedSegments))
	for i := 0; i < len(reversedIndexedSegments); i++ {
		indexedSegments[i] = reversedIndexedSegments[len(reversedIndexedSegments)-i-1]
	}
	version.indexingSegment = indexingSegment
	version.indexedSegments = indexedSegments
	return nil
}

func (writer *writer) loadTailAndRawSegments(ctx countlog.Context, version *EditingStoreVersion) error {
	tailSegment, err := openTailSegment(ctx, writer.store.TailSegmentPath(), writer.store.TailSegmentMaxSize, 0)
	if err != nil {
		return err
	}
	var reversedRawSegments []*rawSegment
	startOffset := tailSegment.startOffset
	for startOffset > version.indexingSegment.tailOffset {
		rawSegmentPath := writer.store.RawSegmentPath(startOffset)
		rawSegment, err := openRawSegment(ctx, rawSegmentPath)
		if err != nil {
			countlog.Error("event!lstore.failed to open raw segment",
				"err", err, "rawSegmentPath", rawSegmentPath,
				"headSegmentTailOffset", version.indexingSegment.tailOffset)
			return err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		startOffset = rawSegment.startOffset
	}
	rawSegments := make([]*rawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	writer.tailSegment = tailSegment
	version.tailSegment = tailSegment.rawSegment
	version.rawSegments = rawSegments
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
		if len(writer.rows) >= blockLength {
			err := writer.rotateTail(ctx, writer.currentVersion)
			ctx.TraceCall("callee!writer.rotate", err)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return
			}
		}
		seq, err := writer.tryWrite(ctx, entry)
		if err == SegmentOverflowError {
			err := writer.rotateTail(ctx, writer.currentVersion)
			ctx.TraceCall("callee!writer.rotate", err)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return
			}
			seq, err = writer.tryWrite(ctx, entry)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return
			}
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
				return writer.store.getTailOffset()
			})
		return result.Offset, result.Error
	case <-ctx.Done():
		ctx.DebugCall("callee!writer.AsyncWrite", ctx.Err())
		return 0, ctx.Err()
	}
}

func (writer *writer) tryWrite(ctx countlog.Context, entry *Entry) (Offset, error) {
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(writer.writeBuf[:0])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return 0, stream.Error
	}
	if size >= uint64(len(writer.writeBuf)) {
		return 0, SegmentOverflowError
	}
	writer.writeBuf = writer.writeBuf[size:]
	offset := writer.startOffset + Offset(len(writer.rows))
	writer.rows = append(writer.rows, entry)
	countlog.Trace("event!writer.tryWrite", "offset", offset)
	// reader will know if read the tail using atomic
	writer.updateTailOffset(offset + 1)
	return offset, nil
}

func (writer *writer) rotateTail(ctx countlog.Context, oldVersion *StoreVersion) error {
	err := writer.tailSegment.Close()
	ctx.TraceCall("callee!tailSegment.Close", err)
	if err != nil {
		return err
	}
	tailSegmentTmpFile, err := createTailSegment(writer.store.TailSegmentTmpPath(), writer.store.TailSegmentMaxSize,
		Offset(writer.store.tailOffset))
	if err != nil {
		return err
	}
	plz.Close(tailSegmentTmpFile)
	newVersion := oldVersion.edit()
	newVersion.rawSegments = make([]*rawSegment, len(oldVersion.rawSegments)+1)
	i := 0
	for ; i < len(oldVersion.rawSegments); i++ {
		newVersion.rawSegments[i] = oldVersion.rawSegments[i]
	}
	rotatedTo := writer.store.RawSegmentPath(Offset(writer.store.tailOffset))
	if err = os.Rename(writer.store.TailSegmentPath(), rotatedTo); err != nil {
		return err
	}
	if err = os.Rename(writer.store.TailSegmentTmpPath(), writer.store.TailSegmentPath()); err != nil {
		return err
	}
	// use writer.tailRows to build a raw segment without loading from file
	newVersion.rawSegments[i] = writer.tailSegment.rawSegment
	writer.tailSegment, err = openTailSegment(
		ctx, writer.store.TailSegmentPath(), writer.store.TailSegmentMaxSize, Offset(writer.store.tailOffset))
	if err != nil {
		return err
	}
	newVersion.tailSegment = writer.tailSegment.rawSegment
	sealedNewVersion := newVersion.seal()
	writer.updateCurrentVersion(sealedNewVersion)
	countlog.Debug("event!store.rotated tail",
		"tail", sealedNewVersion.tailSegment.startOffset,
		"rawSegmentsCount", len(sealedNewVersion.rawSegments),
		"rotatedTo", rotatedTo)
	return nil
}

// purgeRawSegments should only be used by indexer
func (writer *writer) purgeRawSegments(
	ctx countlog.Context, purgedRawSegmentsCount int) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		purgedRawSegments := oldVersion.rawSegments[:purgedRawSegmentsCount]
		for _, segment := range purgedRawSegments {
			segmentPath := writer.store.RawSegmentPath(segment.startOffset + Offset(len(segment.rows)))
			err := os.Remove(segmentPath)
			ctx.TraceCall("callee!os.Remove", err)
		}
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

// rotateIndex should only be used by indexer
func (writer *writer) rotateIndex(
	ctx countlog.Context, indexedSegment *searchable, indexingSegment *indexingSegment) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		newVersion.indexedSegments = append(newVersion.indexedSegments, indexedSegment)
		newVersion.indexingSegment = indexingSegment
		writer.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.rotated index",
		"indexingSegment.startOffset", indexingSegment.startOffset)
	return <-resultChan
}

func (writer *writer) updateCurrentVersion(newVersion *StoreVersion) {
	if newVersion == nil {
		return
	}
	atomic.StorePointer(&writer.store.currentVersion, unsafe.Pointer(newVersion))
	if writer.currentVersion != nil {
		err := writer.currentVersion.Close()
		if err != nil {
			countlog.Error("event!store.failed to close", "err", err)
		}
	}
	writer.currentVersion = newVersion
}

func (writer *writer) updateTailOffset(tailOffset Offset) {
	atomic.StoreUint64(&writer.store.tailOffset, uint64(tailOffset))
}