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
	*tailSegment
	store          *Store
	commandQueue   chan writerCommand
	currentVersion *StoreVersion
	rawSegments    []*rawSegment
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
	ctx.Info("event!writer.loadInitialVersion",
		"indexingTailOffset", writer.currentVersion.indexingSegment.tailOffset,
		"storeTailOffset", writer.store.tailOffset)
	return nil
}

func (writer *writer) loadInitialVersion(ctx countlog.Context) error {
	version := &StoreVersion{}
	err := writer.loadIndexingAndIndexedChunks(ctx, version)
	ctx.TraceCall("callee!writer.loadIndexingAndIndexedChunks", err)
	if err != nil {
		return err
	}
	err = writer.loadRawChunks(ctx, version)
	ctx.TraceCall("callee!writer.loadRawChunks", err)
	if err != nil {
		return err
	}
	writer.updateCurrentVersion(version)
	return nil
}

func (writer *writer) loadIndexingAndIndexedChunks(ctx countlog.Context, version *StoreVersion) error {
	indexingSegment, err := openIndexSegment(
		ctx, writer.store.IndexingSegmentPath())
	if os.IsNotExist(err) {
		slotIndexWriter := writer.store.slotIndexManager.newWriter(10, 4)
		defer plz.Close(slotIndexWriter)
		indexingSegment, err = newIndexSegment(slotIndexWriter, nil)
		if err != nil {
			return err
		}
		err = createIndexSegment(ctx, writer.store.IndexingSegmentPath(), indexingSegment)
		if err != nil {
			return err
		}
	} else if err != nil {
		ctx.TraceCall("callee!store.openIndexingSegment", err)
		return err
	}
	startOffset := indexingSegment.headOffset
	var reversedIndexedSegments []*indexSegment
	for {
		indexedSegmentPath := writer.store.IndexedSegmentPath(startOffset)
		indexedSegment, err := openIndexSegment(ctx, indexedSegmentPath)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		reversedIndexedSegments = append(reversedIndexedSegments, indexedSegment)
		startOffset = indexedSegment.headOffset
	}
	indexedSegments := make([]*indexSegment, len(reversedIndexedSegments))
	for i := 0; i < len(reversedIndexedSegments); i++ {
		indexedSegments[i] = reversedIndexedSegments[len(reversedIndexedSegments)-i-1]
	}
	version.indexedSegments = indexedSegments
	version.indexingSegment = indexingSegment
	return nil
}

func (writer *writer) loadRawChunks(ctx countlog.Context, version *StoreVersion) error {
	tailChunk, entries, err := openTailSegment(
		ctx, writer.store.TailSegmentPath(), writer.store.TailSegmentMaxSize, 0)
	if err != nil {
		return err
	}
	writer.setTailOffset(tailChunk.headOffset + Offset(tailChunk.tailEntriesCount))
	headOffset := tailChunk.headOffset
	var reversedRawSegments []*rawSegment
	offset := tailChunk.headOffset
	indexingSegmentTailOffset := version.indexingSegment.tailOffset
	for offset > indexingSegmentTailOffset {
		rawSegmentPath := writer.store.RawSegmentPath(offset)
		rawSegment, segmentEntries, err := openRawSegment(ctx, rawSegmentPath)
		if err != nil {
			countlog.Error("event!lstore.failed to open raw segment",
				"err", err, "rawSegmentPath", rawSegmentPath,
				"indexingSegmentTailOffset", indexingSegmentTailOffset)
			return err
		}
		reversedRawSegments = append(reversedRawSegments, rawSegment)
		offset = rawSegment.headOffset
		headOffset = rawSegment.headOffset
		entries = append(segmentEntries, entries...)
	}
	rawSegments := make([]*rawSegment, len(reversedRawSegments))
	for i := 0; i < len(reversedRawSegments); i++ {
		rawSegments[i] = reversedRawSegments[len(reversedRawSegments)-i-1]
	}
	rawChunks := newChunks(writer.store.IndexingStrategy, headOffset)
	rawChunk := rawChunks[0]
	for _, entry := range entries {
		if rawChunk.add(entry) {
			rawChunk = newChunk(writer.store.IndexingStrategy, headOffset)
			rawChunks = append(rawChunks, rawChunk)
		}
	}
	writer.tailSegment = tailChunk
	writer.rawSegments = rawSegments
	version.chunks = rawChunks
	return nil
}

func (writer *writer) start() {
	writer.store.executor.Go(func(ctxObj context.Context) {
		ctx := countlog.Ctx(ctxObj)
		defer func() {
			countlog.Info("event!writer.stop")
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
		if writer.tailEntriesCount >= blockLength {
			err := writer.rotateTail(ctx, writer.currentVersion)
			ctx.TraceCall("callee!writer.rotateTail", err)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return
			}
		}
		offset := Offset(writer.store.tailOffset)
		err := writer.tryWrite(ctx, entry)
		if err == nil {
			resultChan <- WriteResult{offset, nil}
			return
		}
		if err == SegmentOverflowError {
			err := writer.rotateTail(ctx, writer.currentVersion)
			ctx.TraceCall("callee!writer.rotate", err)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return
			}
			err = writer.tryWrite(ctx, entry)
			if err != nil {
				resultChan <- WriteResult{0, err}
				return
			}
			resultChan <- WriteResult{offset, nil}
			return
		}
		resultChan <- WriteResult{0, err}
		return
	})
}

func (writer *writer) Write(ctxObj context.Context, entry *Entry) (Offset, error) {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan WriteResult)
	writer.AsyncWrite(ctx, entry, resultChan)
	select {
	case result := <-resultChan:
		ctx.TraceCall("callee!writer.Write", result.Error, "offset", result.Offset)
		return result.Offset, result.Error
	case <-ctx.Done():
		ctx.TraceCall("callee!writer.Write", ctx.Err())
		return 0, ctx.Err()
	}
}

func (writer *writer) tryWrite(ctx countlog.Context, entry *Entry) error {
	stream := ctx.Value("stream").(*gocodec.Stream)
	stream.Reset(writer.writeBuf[:0])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return stream.Error
	}
	if size >= uint32(len(writer.writeBuf)) {
		return SegmentOverflowError
	}
	writer.writeBuf = writer.writeBuf[size:]
	rawChunks := writer.currentVersion.chunks
	tailRawChunk := rawChunks[len(rawChunks)-1]
	if tailRawChunk.add(entry) {
		rawChunks = append(rawChunks, newChunk(
			writer.store.IndexingStrategy, tailRawChunk.tailOffset))
		newVersion := &StoreVersion{
			indexedSegments: append([]*indexSegment(nil), writer.currentVersion.indexedSegments...),
			indexingSegment: writer.currentVersion.indexingSegment,
			chunks:          rawChunks,
		}
		countlog.Debug("event!writer.rotated raw chunk",
			"tailOffset", tailRawChunk.tailOffset)
		writer.updateCurrentVersion(newVersion)
	}
	// reader will know if read the tail using atomic
	writer.incrementTailOffset()
	return nil
}

func (writer *writer) rotateTail(ctx countlog.Context, oldVersion *StoreVersion) error {
	oldTailSegmentHeadOffset := writer.tailSegment.headOffset
	err := writer.tailSegment.Close()
	ctx.TraceCall("callee!tailSegment.Close", err)
	if err != nil {
		return err
	}
	newTailSegment, _, err := openTailSegment(ctx, writer.store.TailSegmentTmpPath(), writer.store.TailSegmentMaxSize,
		Offset(writer.store.tailOffset))
	if err != nil {
		return err
	}
	writer.tailSegment = newTailSegment
	rotatedTo := writer.store.RawSegmentPath(Offset(writer.store.tailOffset))
	if err = os.Rename(writer.store.TailSegmentPath(), rotatedTo); err != nil {
		return err
	}
	if err = os.Rename(writer.store.TailSegmentTmpPath(), writer.store.TailSegmentPath()); err != nil {
		return err
	}
	writer.rawSegments = append(writer.rawSegments, &rawSegment{
		segmentHeader: segmentHeader{segmentTypeRaw, oldTailSegmentHeadOffset},
		path:          rotatedTo,
	})
	countlog.Debug("event!writer.rotated tail",
		"rotatedTo", rotatedTo)
	return nil
}

// movedBlockIntoIndex should only be used by indexer
func (writer *writer) movedBlockIntoIndex(
	ctx countlog.Context, indexingSegment *indexSegment) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := &StoreVersion{
			indexedSegments: append([]*indexSegment(nil), oldVersion.indexedSegments...),
			indexingSegment: indexingSegment,
			chunks:          append([]*chunk(nil), oldVersion.chunks...),
		}
		firstRawChunk := *newVersion.chunks[0] // copy the first raw chunk
		firstRawChunk.headSlot += 4
		if firstRawChunk.headSlot == 64 {
			newVersion.chunks = newVersion.chunks[1:]
		} else {
			newVersion.chunks[0] = &firstRawChunk
		}
		headOffset := newVersion.chunks[0].headOffset + Offset(newVersion.chunks[0].headSlot<<6)
		removedRawSegmentsCount := 0
		for i, rawSegment := range writer.rawSegments {
			if rawSegment.headOffset <= headOffset {
				removedRawSegmentsCount = i
			} else {
				break
			}
		}
		removedRawSegments := writer.rawSegments[:removedRawSegmentsCount]
		writer.rawSegments = writer.rawSegments[removedRawSegmentsCount:]
		for _, removedRawSegment := range removedRawSegments {
			err := os.Remove(removedRawSegment.path)
			ctx.TraceCall("callee!os.Remove", err,
				"path", removedRawSegment.path)
		}
		writer.updateCurrentVersion(newVersion)
		resultChan <- nil
		countlog.Debug("event!writer.movedBlockIntoIndex",
			"indexingSegment.headOffset", indexingSegment.headOffset,
				"removedRawSegmentsCount", removedRawSegmentsCount)
		return
	})
	return <-resultChan
}

// rotatedIndex should only be used by indexer
func (writer *writer) rotatedIndex(
	ctx countlog.Context, indexedSegment *indexSegment, indexingSegment *indexSegment) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := &StoreVersion{
			indexedSegments: append(oldVersion.indexedSegments, indexedSegment),
			indexingSegment: indexingSegment,
			chunks:          oldVersion.chunks,
		}
		writer.updateCurrentVersion(newVersion)
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.rotated index",
		"indexingSegment.headOffset", indexingSegment.headOffset)
	return <-resultChan
}

// removedIndex should only be used by indexer
func (writer *writer) removedIndex(
	ctx countlog.Context, removedIndexedChunksCount int) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := &StoreVersion{
			indexedSegments: oldVersion.indexedSegments[removedIndexedChunksCount:],
			indexingSegment: oldVersion.indexingSegment,
			chunks:          oldVersion.chunks,
		}
		writer.updateCurrentVersion(newVersion)
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.rotated index",
		"removedIndexedChunksCount", removedIndexedChunksCount)
	return <-resultChan
}

func (writer *writer) updateCurrentVersion(newVersion *StoreVersion) {
	if newVersion == nil {
		return
	}
	atomic.StorePointer(&writer.store.currentVersion, unsafe.Pointer(newVersion))
	writer.currentVersion = newVersion
}

func (writer *writer) incrementTailOffset() {
	writer.tailEntriesCount += 1
	atomic.StoreUint64(&writer.store.tailOffset, writer.store.tailOffset+1)
}

func (writer *writer) setTailOffset(tailOffset Offset) {
	atomic.StoreUint64(&writer.store.tailOffset, uint64(tailOffset))
}
