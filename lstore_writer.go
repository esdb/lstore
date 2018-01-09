package lstore

import (
	"github.com/v2pro/plz/countlog"
	"context"
	"os"
	"github.com/v2pro/plz/concurrent"
	"unsafe"
	"sync/atomic"
	"github.com/esdb/gocodec"
)

type writerCommand func(ctx countlog.Context)

type writer struct {
	*tailChunk
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
	return writer.tailChunk.Close()
}

func (writer *writer) load(ctx countlog.Context) error {
	err := writer.loadInitialVersion(ctx)
	if err != nil {
		return err
	}
	ctx.Info("event!writer.loadInitialVersion",
		"indexingTailOffset", writer.currentVersion.indexingChunk.tailOffset,
		"tailHeadOffset", writer.tailChunk.headOffset,
		"storeTailOffset", writer.store.tailOffset,
			"rawChunkHeadOffset", writer.currentVersion.rawChunks[0].headOffset)
	return nil
}

func (writer *writer) loadInitialVersion(ctx countlog.Context) error {
	version := StoreVersion{}.edit()
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
	writer.updateCurrentVersion(version.seal())
	return nil
}

func (writer *writer) loadIndexingAndIndexedChunks(ctx countlog.Context, version *EditingStoreVersion) error {
	indexingSegment, err := openIndexSegment(
		ctx, writer.store.IndexingSegmentPath())
	if os.IsNotExist(err) {
		indexingSegment, err = newIndexSegment(writer.store.slotIndexManager, nil)
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
	var reversedIndexedChunks []*indexChunk
	for {
		indexedSegmentPath := writer.store.IndexedSegmentPath(startOffset)
		indexedSegment, err := openIndexSegment(ctx, indexedSegmentPath)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		reversedIndexedChunks = append(reversedIndexedChunks, &indexChunk{
			indexSegment:  indexedSegment,
			blockManager:  writer.store.blockManager,
			slotIndexManager: writer.store.slotIndexManager,
		})
		startOffset = indexedSegment.headOffset
	}
	indexedChunks := make([]*indexChunk, len(reversedIndexedChunks))
	for i := 0; i < len(reversedIndexedChunks); i++ {
		indexedChunks[i] = reversedIndexedChunks[len(reversedIndexedChunks)-i-1]
	}
	version.indexedChunks = indexedChunks
	version.indexingChunk = &indexChunk{
		indexSegment:  indexingSegment,
		blockManager:  writer.store.blockManager,
		slotIndexManager: writer.store.slotIndexManager,
	}
	return nil
}

func (writer *writer) loadRawChunks(ctx countlog.Context, version *EditingStoreVersion) error {
	tailChunk, entries, err := openTailChunk(
		ctx, writer.store.TailSegmentPath(), writer.store.TailSegmentMaxSize, 0)
	if err != nil {
		return err
	}
	writer.setTailOffset(tailChunk.headOffset + Offset(tailChunk.tailEntriesCount))
	headOffset := tailChunk.headOffset
	var reversedRawSegments []*rawSegment
	offset := tailChunk.headOffset
	indexingSegmentTailOffset := version.indexingChunk.tailOffset
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
	rawChunks := newRawChunks(writer.store.IndexingStrategy, headOffset)
	rawChunk := rawChunks[0]
	for _, entry := range entries {
		if rawChunk.add(entry) {
			rawChunk = newRawChunk(writer.store.IndexingStrategy, headOffset)
			rawChunks = append(rawChunks, rawChunk)
		}
	}
	writer.tailChunk = tailChunk
	writer.rawSegments = rawSegments
	version.rawChunks = rawChunks
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
			ctx.TraceCall("callee!writer.rotate", err)
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
	rawChunks := writer.currentVersion.rawChunks
	tailRawChunk := rawChunks[len(rawChunks)-1]
	if tailRawChunk.add(entry) {
		writer.currentVersion.rawChunks = append(rawChunks, newRawChunk(
			writer.store.IndexingStrategy, tailRawChunk.headOffset + 4096))
	}
	// reader will know if read the tail using atomic
	countlog.Trace("event!writer.tryWrite", "offset", writer.store.tailOffset)
	writer.incrementTailOffset()
	return nil
}

func (writer *writer) rotateTail(ctx countlog.Context, oldVersion *StoreVersion) error {
	err := writer.tailChunk.Close()
	ctx.TraceCall("callee!tailChunk.Close", err)
	if err != nil {
		return err
	}
	newTailChunk, _, err := openTailChunk(ctx, writer.store.TailSegmentTmpPath(), writer.store.TailSegmentMaxSize,
		Offset(writer.store.tailOffset))
	if err != nil {
		return err
	}
	writer.tailChunk = newTailChunk
	rotatedTo := writer.store.RawSegmentPath(Offset(writer.store.tailOffset))
	if err = os.Rename(writer.store.TailSegmentPath(), rotatedTo); err != nil {
		return err
	}
	if err = os.Rename(writer.store.TailSegmentTmpPath(), writer.store.TailSegmentPath()); err != nil {
		return err
	}
	writer.rawSegments = append(writer.rawSegments, &rawSegment{
		segmentHeader: segmentHeader{segmentTypeRaw, Offset(writer.store.tailOffset)},
		path:          rotatedTo,
	})
	countlog.Debug("event!store.rotated tail",
		"rotatedTo", rotatedTo)
	return nil
}

// movedBlockIntoIndex should only be used by indexer
func (writer *writer) movedBlockIntoIndex(
	ctx countlog.Context, indexingChunk *indexChunk) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		firstRawChunk := *newVersion.rawChunks[0] // copy the first raw chunk
		firstRawChunk.headSlot += 4
		if firstRawChunk.headSlot == 64 {
			newVersion.rawChunks = newVersion.rawChunks[1:]
		} else {
			newVersion.rawChunks[0] = &firstRawChunk
		}
		headOffset := newVersion.rawChunks[0].headOffset + Offset(newVersion.rawChunks[0].headSlot<<6)
		removedRawSegmentsCount := 0
		for i, rawSegment := range writer.rawSegments {
			if rawSegment.headOffset <= headOffset {
				removedRawSegmentsCount = i
				break
			}
		}
		removedRawSegments := writer.rawSegments[:removedRawSegmentsCount]
		writer.rawSegments = writer.rawSegments[removedRawSegmentsCount:]
		for _, removedRawSegment := range removedRawSegments {
			err := os.Remove(removedRawSegment.path)
			ctx.TraceCall("callee!os.Remove", err)
		}
		newVersion.indexingChunk = indexingChunk
		writer.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.movedBlockIntoIndex",
		"indexingChunk.headOffset", indexingChunk.headOffset)
	return <-resultChan
}

// rotatedIndex should only be used by indexer
func (writer *writer) rotatedIndex(
	ctx countlog.Context, indexedChunk *indexChunk, indexingChunk *indexChunk) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		newVersion.indexedChunks = append(newVersion.indexedChunks, indexedChunk)
		newVersion.indexingChunk = indexingChunk
		writer.updateCurrentVersion(newVersion.seal())
		resultChan <- nil
		return
	})
	countlog.Debug("event!writer.rotated index",
		"indexingChunk.headOffset", indexingChunk.headOffset)
	return <-resultChan
}

// removedIndex should only be used by indexer
func (writer *writer) removedIndex(
	ctx countlog.Context, removedIndexedChunksCount int) error {
	resultChan := make(chan error)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		oldVersion := writer.currentVersion
		newVersion := oldVersion.edit()
		newVersion.indexedChunks = newVersion.indexedChunks[removedIndexedChunksCount:]
		writer.updateCurrentVersion(newVersion.seal())
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
