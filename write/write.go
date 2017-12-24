package write

import (
	"github.com/esdb/lstore"
	"context"
	"errors"
	"github.com/esdb/gocodec"
)

var SegmentOverflowError = errors.New("please rotate to new segment")

type Result struct {
	Offset lstore.Offset
	Error  error
}

func AsyncExecute(ctx context.Context, store *lstore.Store, entry *lstore.Entry, resultChan chan<- Result) {
	store.AsyncExecute(ctx, func(store *lstore.StoreVersion) *lstore.StoreVersion {
		rotatedStore, result := writeOrRotate(store, entry)
		resultChan <- result
		return rotatedStore
	})
}

func Execute(ctx context.Context, store *lstore.Store, entry *lstore.Entry) (lstore.Offset, error) {
	resultChan := make(chan Result)
	AsyncExecute(ctx, store, entry, resultChan)
	select {
	case result := <-resultChan:
		return result.Offset, result.Error
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func writeOrRotate(store *lstore.StoreVersion, entry *lstore.Entry) (*lstore.StoreVersion, Result) {
	result := tryWrite(store.Tail(), entry)
	if result.Error == SegmentOverflowError {
		rotatedStore, err := store.AddSegment()
		if err != nil {
			return nil, Result{0, err}
		}
		result = tryWrite(rotatedStore.Tail(), entry)
		return rotatedStore, result
	}
	return nil, result
}

func tryWrite(segment *lstore.Segment, entry *lstore.Entry) Result {
	buf := segment.WriteBuffer()
	maxTail := segment.StartOffset + lstore.Offset(len(buf))
	offset := segment.Tail
	if offset >= maxTail {
		return Result{0, SegmentOverflowError}
	}
	stream := gocodec.NewStream(buf[offset-segment.StartOffset:offset-segment.StartOffset])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return Result{0, stream.Error}
	}
	tail := offset + lstore.Offset(size)
	if tail >= maxTail {
		return Result{0, SegmentOverflowError}
	}
	segment.Tail = tail
	return Result{offset, nil}
}
