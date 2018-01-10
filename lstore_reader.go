package lstore

import (
	"github.com/esdb/gocodec"
	"context"
	"github.com/v2pro/plz/countlog"
	"errors"
)

// Reader is not thread safe, can only be used from one goroutine
type Reader struct {
	store          *Store
	currentVersion *StoreVersion
	tailOffset     Offset
	gocIter        *gocodec.Iterator
}

// SearchAborted should be returned if you want to end the scanForward from callback
var SearchAborted = errors.New("scanForward aborted")

type SearchCallback interface {
	HandleRow(offset Offset, entry *Entry) error
}

func (store *Store) NewReader(ctxObj context.Context) (*Reader, error) {
	ctx := countlog.Ctx(ctxObj)
	reader := &Reader{
		store:   store,
		gocIter: gocodec.NewIterator(nil),
	}
	reader.Refresh(ctx)
	store.blockManager.diskManager.Lock(reader)
	store.slotIndexManager.diskManager.Lock(reader)
	return reader, nil
}

func (reader *Reader) TailOffset() Offset {
	return reader.tailOffset
}

// Refresh has minimum cost of two cas read, one for store.latestVersion, one for tailChunk.tail
func (reader *Reader) Refresh(ctx context.Context) bool {
	hasNew := false
	newTailOffset := reader.store.getTailOffset()
	if newTailOffset != reader.tailOffset {
		reader.tailOffset = newTailOffset
		hasNew = true
	}
	latestVersion := reader.store.latest()
	if reader.currentVersion != latestVersion {
		reader.currentVersion = latestVersion
		hasNew = true
	}
	return hasNew
}

func (reader *Reader) Close() error {
	reader.store.blockManager.diskManager.Unlock(reader)
	reader.store.slotIndexManager.diskManager.Unlock(reader)
	return nil
}

func (reader *Reader) SearchForward(ctxObj context.Context, startOffset Offset, filter Filter, cb SearchCallback) error {
	ctx := countlog.Ctx(ctxObj)
	store := reader.currentVersion
	if filter == nil {
		filter = dummyFilterInstance
	}
	for _, indexedSegment := range store.indexedChunks {
		if err := indexedSegment.searchForward(ctx, startOffset, filter, cb); err != nil {
			return err
		}
	}
	if startOffset < store.indexingChunk.tailOffset {
		if err := store.indexingChunk.searchForward(ctx, startOffset, filter, cb); err != nil {
			return err
		}
		startOffset = store.indexingChunk.tailOffset
	}
	lastRawChunk := len(store.rawChunks) - 1
	for _, rawSegmentIndex := range store.rawChunks[:lastRawChunk] {
		if err := rawSegmentIndex.searchForward(ctx, startOffset, filter, cb); err != nil {
			return err
		}
	}
	if err := store.rawChunks[lastRawChunk].searchForward(
		ctx, startOffset, filter, &StopAt{reader.tailOffset, cb}); err != nil {
		return err
	}
	return nil
}

type RowsCollector struct {
	LimitSize int
	Rows      []Row
}

func (collector *RowsCollector) HandleRow(offset Offset, entry *Entry) error {
	collector.Rows = append(collector.Rows, Row{Offset: offset, Entry: entry})
	if len(collector.Rows) == collector.LimitSize {
		return SearchAborted
	}
	return nil
}

type OffsetsCollector struct {
	LimitSize int
	Offsets   []Offset
}

func (collector *OffsetsCollector) HandleRow(offset Offset, entry *Entry) error {
	collector.Offsets = append(collector.Offsets, offset)
	if len(collector.Offsets) == collector.LimitSize {
		return SearchAborted
	}
	return nil
}

type StopAt struct {
	Offset         Offset
	SearchCallback SearchCallback
}

func (collector *StopAt) HandleRow(offset Offset, entry *Entry) error {
	if offset >= collector.Offset {
		return SearchAborted
	}
	return collector.SearchCallback.HandleRow(offset, entry)
}
