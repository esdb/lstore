package lstore

import (
	"github.com/v2pro/plz/countlog"
	"os"
	"sync/atomic"
	"context"
)

func (writer *appender) BatchAppend(ctxObj context.Context, resultChan chan<- AppendResult, entries []*Entry) {
	ctx := countlog.Ctx(ctxObj)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		for _, entry := range entries {
			writer.writeOne(ctx, resultChan, entry)
		}
	})
}

func (writer *appender) writeOne(ctx countlog.Context, resultChan chan<- AppendResult, entry *Entry) {
	offset := Offset(writer.state.tailOffset)
	err := writer.tryAppend(ctx, entry)
	if err == nil {
		resultChan <- AppendResult{offset, nil}
		return
	}
	if err == SegmentOverflowError {
		err := writer.rotateTail(ctx)
		ctx.TraceCall("callee!appender.rotate", err)
		if err != nil {
			resultChan <- AppendResult{0, err}
			return
		}
		err = writer.tryAppend(ctx, entry)
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

func (writer *appender) Append(ctxObj context.Context, entry *Entry) (Offset, error) {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan AppendResult)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		writer.writeOne(ctx, resultChan, entry)
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

func (writer *appender) tryAppend(ctx countlog.Context, entry *Entry) error {
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
	if writer.appendingChunk.add(entry) || writer.appendingChunk.tailSlot == writer.chunkMaxSlot {
		writer.appendingChunk = newChunk(writer.strategy, writer.appendingChunk.tailOffset)
		writer.state.rotatedChunk(writer.appendingChunk)
	}
	// reader will know if read the tail using atomic
	writer.incrementTailOffset()
	return nil
}

func (writer *appender) rotateTail(ctx countlog.Context) error {
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
	countlog.Debug("event!appender.rotated raw segment",
		"rotatedTo", rotatedTo)
	return nil
}

func (writer *appender) incrementTailOffset() {
	writer.tailEntriesCount += 1
	atomic.StoreUint64(&writer.state.tailOffset, writer.state.tailOffset+1)
}

func (writer *appender) setTailOffset(tailOffset Offset) {
	atomic.StoreUint64(&writer.state.tailOffset, uint64(tailOffset))
}
