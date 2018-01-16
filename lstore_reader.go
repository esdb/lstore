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
	state           *storeState
	currentVersion  *storeVersion
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
var SearchAborted = errors.New("search aborted")

type SearchCallback interface {
	HandleRow(offset Offset, entry *Entry) error
}

func (store *Store) NewBlobValueFilter(column int, value Blob) Filter {
	return store.strategy.NewBlobValueFilter(column, value)
}

func (store *Store) NewReader(ctxObj context.Context) (*Reader, error) {
	ctx := countlog.Ctx(ctxObj)
	reader := &Reader{
		state:           &store.storeState,
		slotIndexReader: store.slotIndexManager.newReader(14, 4),
		blockReader:     store.blockManager.newReader(14, 4),
	}
	reader.RefreshTail(ctx)
	store.lockHead(reader)
	return reader, nil
}

func (reader *Reader) TailOffset() Offset {
	return reader.tailOffset
}

func (reader *Reader) Refresh() (headMoved bool, tailMoved bool) {
	newTailOffset := reader.state.getTailOffset()
	if newTailOffset != reader.tailOffset {
		reader.tailOffset = newTailOffset
		tailMoved = true
	}
	oldHeadOffset := reader.currentVersion.HeadOffset()
	reader.currentVersion = reader.state.lockHead(reader)
	if reader.currentVersion.HeadOffset() != oldHeadOffset {
		headMoved = true
	}
	return headMoved, tailMoved
}

// RefreshTail has minimum cost of two cas read, one for store.latestVersion, one for tailSegment.tail
func (reader *Reader) RefreshTail(ctx context.Context) (tailMoved bool) {
	newTailOffset := reader.state.getTailOffset()
	if newTailOffset != reader.tailOffset {
		reader.tailOffset = newTailOffset
		tailMoved = true
	}
	reader.currentVersion = reader.state.latest()
	return tailMoved
}

func (reader *Reader) Close() error {
	reader.state.unlockHead(reader)
	return plz.CloseAll([]io.Closer{
		reader.slotIndexReader,
		reader.blockReader,
	})
}

func (reader *Reader) SearchForward(ctxObj context.Context, req *SearchRequest) error {
	ctx := countlog.Ctx(ctxObj)
	store := reader.currentVersion
	reader.slotIndexReader.gc()
	reader.blockReader.gc()
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
	for _, chunk := range store.appendedChunks {
		if err := chunk.searchForward(ctx, req); err != nil {
			return err
		}
	}
	req.Callback = &StopAt{reader.tailOffset, req.Callback}
	if err := store.appendingChunk.searchForward(ctx, req); err != nil {
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
