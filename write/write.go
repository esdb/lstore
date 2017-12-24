package write

import (
	"github.com/esdb/lstore"
	"context"
)


type Result struct {
	Offset lstore.Offset
	Error  error
}

func AsyncExecute(ctx context.Context, store *lstore.Store, entry *lstore.Entry, resultChan chan<- Result) {
	store.AsyncExecute(ctx, func(ctx context.Context, store *lstore.StoreVersion) *lstore.StoreVersion {
		rotatedStore, result := doWrite(ctx, store, entry)
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

func doWrite(ctx context.Context, store *lstore.StoreVersion, entry *lstore.Entry) (*lstore.StoreVersion, Result) {
	offset, err := store.TailSegment().Write(ctx, entry)
	if err == lstore.SegmentOverflowError {
		rotatedStore, err := store.AddSegment()
		if err != nil {
			return nil, Result{0, err}
		}
		offset, err = rotatedStore.TailSegment().Write(ctx, entry)
		return rotatedStore, Result{offset, nil}
	}
	return nil, Result{offset, nil}
}

