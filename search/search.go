package search

import (
	"context"
	"github.com/esdb/lstore"
	"github.com/esdb/gocodec"
)

type Row struct {
	*lstore.Entry
	Offset lstore.Offset
}

type Iterator struct {
	store *lstore.StoreVersion
	gocIter *gocodec.Iterator
	tailSegmentSize int
	batchSize int
	filters []Filter
}

func (iter *Iterator) Next()([]Row, error) {
	gocIter := iter.gocIter
	var batch []Row
	for i := 0; i < iter.batchSize; i++ {
		offset := lstore.Offset(iter.tailSegmentSize - len(gocIter.Buffer()))
		entry, _ := gocIter.Unmarshal((*lstore.Entry)(nil)).(*lstore.Entry)
		if gocIter.Error != nil {
			return nil, gocIter.Error
		}
		if iter.matches(entry) {
			batch = append(batch, Row{Offset: offset, Entry: entry})
		}
	}
	return batch, nil
}

func (iter *Iterator) matches(entry *lstore.Entry) bool {
	for _, filter := range iter.filters {
		if !filter.matches(entry) {
			return false
		}
	}
	return true
}

func (iter *Iterator) Close() error {
	return iter.store.Close()
}

func Execute(ctx context.Context, storeHolder *lstore.Store,
	startOffset lstore.Offset, batchSize int, filters ...Filter) *Iterator {
	store := storeHolder.Latest()
	mapped := store.Tail().ReadMMap()
	gocIter := gocodec.NewIterator(mapped[startOffset:])
	return &Iterator{
		store: store,
		gocIter: gocIter,
		tailSegmentSize: len(mapped),
		batchSize: batchSize,
		filters: filters,
	}
}
