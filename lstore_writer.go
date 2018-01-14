package lstore

import (
	"github.com/v2pro/plz/countlog"
	"context"
	"os"
	"github.com/v2pro/plz/concurrent"
	"github.com/esdb/gocodec"
)

type writerCommand func(ctx countlog.Context)

type writer struct {
	*tailSegment
	cfg            *writerConfig
	strategy       *indexingStrategy
	state          *storeState
	commandQueue   chan writerCommand
	currentVersion *storeVersion
	rawSegments    []*rawSegment
	stream         *gocodec.Stream
}

type WriteResult struct {
	Offset Offset
	Error  error
}

func (store *Store) newWriter(ctx countlog.Context) (*writer, error) {
	cfg := store.cfg
	if cfg.WriterCommandQueueLength == 0 {
		cfg.WriterCommandQueueLength = 1024
	}
	if cfg.RawSegmentMaxSizeInBytes == 0 {
		cfg.RawSegmentMaxSizeInBytes = 200 * 1024 * 1024
	}
	writer := &writer{
		state:        &store.storeState,
		stream:       gocodec.NewStream(nil),
		strategy:     store.strategy,
		cfg:          &cfg.writerConfig,
		commandQueue: make(chan writerCommand, cfg.WriterCommandQueueLength),
	}
	writer.updateCurrentVersion(&storeVersion{chunks: newChunks(store.strategy, 0)})
	writer.start(store.executor)
	return writer, nil
}

func (writer *writer) Close() error {
	return writer.tailSegment.Close()
}

func (writer *writer) start(executor *concurrent.UnboundedExecutor) {
	executor.Go(func(ctxObj context.Context) {
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
		offset := Offset(writer.state.tailOffset)
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
	stream := writer.stream
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
		rawChunks = append(rawChunks, newChunk(writer.strategy, tailRawChunk.tailOffset))
		newVersion := &storeVersion{
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

func (writer *writer) rotateTail(ctx countlog.Context, oldVersion *storeVersion) error {
	oldTailSegmentHeadOffset := writer.tailSegment.headOffset
	err := writer.tailSegment.Close()
	ctx.TraceCall("callee!tailSegment.Close", err)
	if err != nil {
		return err
	}
	newTailSegment, _, err := openTailSegment(ctx, writer.cfg.TailSegmentTmpPath(), writer.cfg.RawSegmentMaxSizeInBytes,
		Offset(writer.state.tailOffset))
	if err != nil {
		return err
	}
	writer.tailSegment = newTailSegment
	rotatedTo := writer.cfg.RawSegmentPath(Offset(writer.state.tailOffset))
	if err = os.Rename(writer.cfg.TailSegmentPath(), rotatedTo); err != nil {
		return err
	}
	if err = os.Rename(writer.cfg.TailSegmentTmpPath(), writer.cfg.TailSegmentPath()); err != nil {
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
