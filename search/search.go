package search

import (
	"context"
	"github.com/esdb/lstore"
)

type Row struct {
	*lstore.Entry
	Offset lstore.Offset
}

type Iterator func() ([]Row, error)

func Execute(ctx context.Context, storeHolder *lstore.Store,
	startOffset lstore.Offset, batchSize int, filters ...Filter) Iterator {
	store := storeHolder.Latest()
	defer store.Close()
	return nil
}
