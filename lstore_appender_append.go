package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"os"
	"sync/atomic"
)

func (appender *appender) BatchAppend(ctxObj context.Context, resultChan chan<- AppendResult, entries []*Entry) {
	ctx := countlog.Ctx(ctxObj)
	appender.asyncExecute(ctx, func(ctx *countlog.Context) {
		for _, entry := range entries {
			appender.writeOne(ctx, resultChan, entry)
		}
	})
}

func (appender *appender) writeOne(ctx *countlog.Context, resultChan chan<- AppendResult, entry *Entry) {
	offset := Offset(appender.state.tailOffset)
	err := appender.tryAppend(ctx, entry)
	if err == nil {
		resultChan <- AppendResult{offset, nil}
		return
	}
	if err == SegmentOverflowError {
		err := appender.rotateTail(ctx)
		ctx.TraceCall("callee!appender.rotate", err)
		if err != nil {
			resultChan <- AppendResult{0, err}
			return
		}
		err = appender.tryAppend(ctx, entry)
		if err != nil {
			resultChan <- AppendResult{0, err}
			return
		}
		resultChan <- AppendResult{offset, nil}
		return
	}
	resultChan <- AppendResult{0, err}
	return
}

func (appender *appender) Append(ctxObj context.Context, entry *Entry) (Offset, error) {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan AppendResult)
	appender.asyncExecute(ctx, func(ctx *countlog.Context) {
		appender.writeOne(ctx, resultChan, entry)
	})
	select {
	case result := <-resultChan:
		ctx.TraceCall("callee!appender.Write", result.Error, "offset", result.Offset)
		return result.Offset, result.Error
	case <-ctx.Done():
		ctx.TraceCall("callee!appender.Write", ctx.Err())
		return 0, ctx.Err()
	}
}

func (appender *appender) tryAppend(ctx *countlog.Context, entry *Entry) error {
	stream := appender.stream
	stream.Reset(appender.writeBuf[:0])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return stream.Error
	}
	if size >= uint32(len(appender.writeBuf)) {
		return SegmentOverflowError
	}
	appender.writeBuf = appender.writeBuf[size:]
	if appender.appendingChunk.add(entry) {
		appender.appendingChunk = newChunk(appender.strategy, appender.appendingChunk.tailOffset)
		appender.state.rotatedChunk(appender.appendingChunk)
	}
	if appender.appendingChunk.tailSlot == appender.chunkMaxSlot {
		appender.appendingChunk.tailSlot -= 1
		appender.appendingChunk = newChunk(appender.strategy, appender.appendingChunk.tailOffset)
		appender.state.rotatedChunk(appender.appendingChunk)
	}
	// reader will know if read the tail using atomic
	appender.incrementTailOffset()
	return nil
}

func (appender *appender) rotateTail(ctx *countlog.Context) error {
	oldTailSegmentHeadOffset := appender.tailSegment.headOffset
	err := appender.tailSegment.Close()
	ctx.TraceCall("callee!tailSegment.Close", err)
	if err != nil {
		return err
	}
	newTailSegment, _, err := openTailSegment(ctx, appender.cfg.TailSegmentTmpPath(), appender.cfg.RawSegmentMaxSizeInBytes,
		Offset(appender.state.tailOffset))
	if err != nil {
		return err
	}
	appender.tailSegment = newTailSegment
	rotatedTo := appender.cfg.RawSegmentPath(Offset(appender.state.tailOffset))
	if err = os.Rename(appender.cfg.TailSegmentPath(), rotatedTo); err != nil {
		return err
	}
	if err = os.Rename(appender.cfg.TailSegmentTmpPath(), appender.cfg.TailSegmentPath()); err != nil {
		return err
	}
	appender.rawSegments = append(appender.rawSegments, &rawSegment{
		segmentHeader: segmentHeader{segmentTypeRaw, oldTailSegmentHeadOffset},
		path:          rotatedTo,
	})
	countlog.Debug("event!appender.rotated raw segment",
		"rotatedTo", rotatedTo)
	return nil
}

func (appender *appender) incrementTailOffset() {
	appender.tailEntriesCount += 1
	atomic.StoreUint64(&appender.state.tailOffset, appender.state.tailOffset+1)
}

func (appender *appender) setTailOffset(tailOffset Offset) {
	atomic.StoreUint64(&appender.state.tailOffset, uint64(tailOffset))
}
