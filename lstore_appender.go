package lstore

import (
	"github.com/v2pro/plz/countlog"
	"context"
	"os"
	"github.com/v2pro/plz/concurrent"
	"github.com/esdb/gocodec"
	"errors"
	"github.com/esdb/biter"
)

type appenderCommand func(ctx countlog.Context)

type appender struct {
	*tailSegment
	cfg            *writerConfig
	strategy       *indexingStrategy
	state          *storeState
	commandQueue   chan appenderCommand
	appendingChunk *chunk
	rawSegments    []*rawSegment
	stream         *gocodec.Stream
	chunkMaxSlot   biter.Slot
}

type AppendResult struct {
	Offset Offset
	Error  error
}

func (store *Store) newAppender(ctx countlog.Context) (*appender, error) {
	cfg := store.cfg
	if cfg.WriterCommandQueueLength == 0 {
		cfg.WriterCommandQueueLength = 1024
	}
	if cfg.RawSegmentMaxSizeInBytes == 0 {
		cfg.RawSegmentMaxSizeInBytes = 200 * 1024 * 1024
	}
	chunkMaxSlot := biter.Slot(0)
	if cfg.ChunkMaxEntriesCount > 0 {
		if cfg.ChunkMaxEntriesCount % blockLength != 0 {
			return nil, errors.New("ChunkMaxEntriesCount must be multiplier of 256")
		}
		chunkMaxSlot = biter.Slot(cfg.ChunkMaxEntriesCount / 64 + 1)
	}
	writer := &appender{
		state:        &store.storeState,
		stream:       gocodec.NewStream(nil),
		strategy:     store.strategy,
		cfg:          &cfg.writerConfig,
		chunkMaxSlot: chunkMaxSlot,
		commandQueue: make(chan appenderCommand, cfg.WriterCommandQueueLength),
	}
	writer.start(store.executor)
	return writer, nil
}

func (writer *appender) Close() error {
	return writer.tailSegment.Close()
}

func (writer *appender) start(executor *concurrent.UnboundedExecutor) {
	executor.Go(func(ctxObj context.Context) {
		ctx := countlog.Ctx(ctxObj)
		defer func() {
			countlog.Info("event!appender.stop")
		}()
		countlog.Info("event!appender.start")
		stream := gocodec.NewStream(nil)
		ctx = countlog.Ctx(context.WithValue(ctx, "stream", stream))
		for {
			var cmd appenderCommand
			select {
			case <-ctx.Done():
				return
			case cmd = <-writer.commandQueue:
			}
			handleCommand(ctx, cmd)
		}
	})
}

func (writer *appender) asyncExecute(ctx countlog.Context, cmd appenderCommand) {
	select {
	case writer.commandQueue <- cmd:
	case <-ctx.Done():
	}
}

func handleCommand(ctx countlog.Context, cmd appenderCommand) {
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

func (writer *appender) removeRawSegments(
	ctx countlog.Context, untilOffset Offset) {
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		removedRawSegmentsCount := 0
		for i, rawSegment := range writer.rawSegments {
			if rawSegment.headOffset <= untilOffset {
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
	})
}
