package lstore

import (
	"github.com/esdb/gocodec"
	"context"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"errors"
	"math"
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
	_, err := reader.Refresh(ctx)
	ctx.DebugCall("callee!reader.Refresh", err,
		"caller", "store.NewReader",
		"tailOffset", reader.tailOffset)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (reader *Reader) TailOffset() Offset {
	return reader.tailOffset
}

// Refresh has minimum cost of two cas read, one for store.latestVersion, one for tailSegment.tail
func (reader *Reader) Refresh(ctx context.Context) (bool, error) {
	latestVersion := reader.store.latest()
	defer plz.Close(latestVersion, "ctx", ctx)
	if reader.currentVersion != latestVersion {
		// when reader moves forward, older version has a chance to die
		if reader.currentVersion != nil {
			if err := reader.currentVersion.Close(); err != nil {
				return false, err
			}
		}
		latestVersion.Acquire()
		reader.currentVersion = latestVersion
	}

	newTailOffset := reader.store.getTailOffset()
	taiMoved := newTailOffset != reader.tailOffset
	reader.tailOffset = newTailOffset
	return taiMoved, nil
}

func (reader *Reader) Close() error {
	return reader.currentVersion.Close()
}

func (reader *Reader) SearchForward(ctxObj context.Context, startOffset Offset, filter Filter, cb SearchCallback) error {
	ctx := countlog.Ctx(ctxObj)
	store := reader.currentVersion
	if filter == nil {
		filter = dummyFilterInstance
	}
	for _, indexedSegment := range store.indexedSegments {
		if err := indexedSegment.searchForward(ctx, startOffset, filter, cb); err != nil {
			return err
		}
	}
	if startOffset < store.indexingSegment.tailOffset {
		if err := store.indexingSegment.searchForward(ctx, startOffset, filter, cb); err != nil {
			return err
		}
		startOffset = store.indexingSegment.tailOffset
	}
	for _, rawSegment := range store.rawSegments {
		if err := rawSegment.searchForward(ctx, startOffset, math.MaxUint64, filter, cb); err != nil {
			return err
		}
	}
	return store.tailSegment.searchForward(ctx, startOffset, reader.tailOffset, filter, cb)
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