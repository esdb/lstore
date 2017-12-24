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
		resultChan <- writeToTail(store, entry)
		return nil
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

func writeToTail(store *lstore.StoreVersion, entry *lstore.Entry) Result {
	segment := store.Tail()
	buf := segment.WriteBuffer()
	if segment.Tail >= lstore.Offset(len(buf)) {
		return Result{0, SegmentOverflowError}
	}
	offset := segment.Tail
	stream := gocodec.NewStream(buf[offset:offset])
	size := stream.Marshal(*entry)
	if stream.Error != nil {
		return Result{0, stream.Error}
	}
	segment.Tail = offset + lstore.Offset(size)
	if segment.Tail >= lstore.Offset(len(buf)) {
		return Result{0, SegmentOverflowError}
	}
	return Result{offset, nil}
}
