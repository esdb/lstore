package lstore

import (
	"github.com/v2pro/plz/countlog"
	"os"
	"sync/atomic"
	"context"
)

func (writer *writer) BatchWrite(ctxObj context.Context, resultChan chan<- WriteResult, entries []*Entry) {
	ctx := countlog.Ctx(ctxObj)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		for _, entry := range entries {
			writer.writeOne(ctx, resultChan, entry)
		}
	})
}

func (writer *writer) writeOne(ctx countlog.Context, resultChan chan<- WriteResult, entry *Entry) {
	offset := Offset(writer.state.tailOffset)
	err := writer.tryWrite(ctx, entry)
	if err == nil {
		resultChan <- WriteResult{offset, nil}
		return
	}
	if err == SegmentOverflowError {
		err := writer.rotateTail(ctx)
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
}

func (writer *writer) Write(ctxObj context.Context, entry *Entry) (Offset, error) {
	ctx := countlog.Ctx(ctxObj)
	resultChan := make(chan WriteResult)
	writer.asyncExecute(ctx, func(ctx countlog.Context) {
		writer.writeOne(ctx, resultChan, entry)
	})
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
	if writer.appendingChunk.add(entry) || writer.appendingChunk.tailSlot == writer.chunkMaxSlot {
		writer.appendingChunk = newChunk(writer.strategy, writer.appendingChunk.tailOffset)
		writer.state.rotatedChunk(writer.appendingChunk)
	}
	// reader will know if read the tail using atomic
	writer.incrementTailOffset()
	return nil
}

func (writer *writer) rotateTail(ctx countlog.Context) error {
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
	countlog.Debug("event!writer.rotated raw segment",
		"rotatedTo", rotatedTo)
	return nil
}

func (writer *writer) incrementTailOffset() {
	writer.tailEntriesCount += 1
	atomic.StoreUint64(&writer.state.tailOffset, writer.state.tailOffset+1)
}

func (writer *writer) setTailOffset(tailOffset Offset) {
	atomic.StoreUint64(&writer.state.tailOffset, uint64(tailOffset))
}
