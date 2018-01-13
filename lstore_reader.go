package lstore

import (
	"context"
	"github.com/v2pro/plz/countlog"
	"errors"
	"github.com/v2pro/plz"
	"io"
)

// Reader is not thread safe, can only be used from one goroutine
type Reader struct {
	store           *Store
	currentVersion  *StoreVersion
	tailOffset      Offset
	slotIndexReader slotIndexReader
	blockReader     blockReader
}

type SearchRequest struct {
	StartOffset Offset
	Filter      Filter
	Callback    SearchCallback
}

// SearchAborted should be returned if you want to end the scanForward from callback
var SearchAborted = errors.New("scanForward aborted")

type SearchCallback interface {
	HandleRow(offset Offset, entry *Entry) error
}

func (store *Store) NewReader(ctxObj context.Context) (*Reader, error) {
	ctx := countlog.Ctx(ctxObj)
	reader := &Reader{
		store:         store,
		slotIndexReader: store.slotIndexManager.newReader(14, 4),
		blockReader: store.blockManager.newReader(14, 4),
	}
	reader.Refresh(ctx)
	store.blockManager.lock(reader, reader.currentVersion.HeadOffset())
	store.slotIndexManager.lock(reader, reader.currentVersion.HeadOffset())
	return reader, nil
}

func (reader *Reader) TailOffset() Offset {
	return reader.tailOffset
}

// Refresh has minimum cost of two cas read, one for store.latestVersion, one for tailSegment.tail
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
	reader.store.blockManager.unlock(reader)
	reader.store.slotIndexManager.unlock(reader)
	return plz.CloseAll([]io.Closer{
		reader.slotIndexReader,
		reader.blockReader,
	})
}

func (reader *Reader) SearchForward(ctxObj context.Context, req *SearchRequest) error {
	ctx := countlog.Ctx(ctxObj)
	store := reader.currentVersion
	if req.Filter == nil {
		req.Filter = dummyFilterInstance
	}
	for _, indexedSegment := range store.indexedSegments {
		if err := indexedSegment.searchForward(ctx, reader.slotIndexReader, reader.blockReader, req); err != nil {
			return err
		}
	}
	if req.StartOffset < store.indexingSegment.tailOffset {
		if err := store.indexingSegment.searchForward(ctx, reader.slotIndexReader, reader.blockReader, req); err != nil {
			return err
		}
		req.StartOffset = store.indexingSegment.tailOffset
	}
	lastChunk := len(store.chunks) - 1
	for _, chunk := range store.chunks[:lastChunk] {
		if err := chunk.searchForward(ctx, req); err != nil {
			return err
		}
	}
	req.Callback = &StopAt{reader.tailOffset, req.Callback}
	if err := store.chunks[lastChunk].searchForward(ctx, req); err != nil {
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
