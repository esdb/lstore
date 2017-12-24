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
	store         *lstore.StoreVersion
	gocIter       *gocodec.Iterator
	buf           []byte
	currentOffset lstore.Offset
	batchSize     int
	filters       []Filter
}

func (iter *Iterator) Next() ([]Row, error) {
	gocIter := iter.gocIter
	var batch []Row
	for i := 0; i < iter.batchSize; i++ {
		buf := iter.buf[iter.currentOffset:]
		bufSize := len(buf)
		if bufSize < 8 {
			return nil, nil // done
		}
		gocIter.Reset(buf)
		nextEntrySize := lstore.Offset(gocIter.NextSize())
		if nextEntrySize == 0 {
			return batch, nil // done
		}
		// copy next entry from mmap, as mmap is readonly in this case
		gocIter.Reset(append([]byte(nil), iter.buf[iter.currentOffset:iter.currentOffset+nextEntrySize]...))
		entry, _ := gocIter.Unmarshal((*lstore.Entry)(nil)).(*lstore.Entry)
		if gocIter.Error != nil {
			return nil, gocIter.Error
		}
		if iter.matches(entry) {
			batch = append(batch, Row{Offset: iter.currentOffset, Entry: entry})
		}
		iter.currentOffset += nextEntrySize
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
	buf := store.Tail().ReadBuffer()
	gocIter := gocodec.NewIterator(nil)
	return &Iterator{
		store:         store,
		gocIter:       gocIter,
		buf:           buf,
		currentOffset: startOffset,
		batchSize:     batchSize,
		filters:       filters,
	}
}
