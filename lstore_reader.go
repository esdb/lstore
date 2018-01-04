package lstore

import (
	"github.com/esdb/gocodec"
	"context"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/plz"
	"errors"
)

// Reader is not thread safe, can only be used from one goroutine
type Reader struct {
	store          *Store
	currentVersion *StoreVersion
	tailOffset     Offset
	gocIter        *gocodec.Iterator
}

// SearchAborted should be returned if you want to end the search from callback
var SearchAborted = errors.New("search aborted")

type SearchCallback interface {
	HandleRow(offset Offset, entry *Entry) error
}

func (store *Store) NewReader(ctxObj context.Context) (*Reader, error) {
	ctx := countlog.Ctx(ctxObj)
	reader := &Reader{
		store:    store,
		gocIter:  gocodec.NewIterator(nil),
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
	return reader.store.getTailOffset() != reader.tailOffset, nil
}

func (reader *Reader) Close() error {
	return reader.currentVersion.Close()
}

func (reader *Reader) SearchForward(ctxObj context.Context, startOffset Offset, filters []Filter, cb SearchCallback) error {
	ctx := countlog.Ctx(ctxObj)
	store := reader.currentVersion
	if err := store.indexingSegment.searchForward(ctx, startOffset, filters, cb); err != nil {
		return err
	}
	for _, rawSegment := range store.rawSegments {
		if err := rawSegment.search(ctx, startOffset, filters, cb); err != nil {
			return err
		}
	}
	return store.tailSegment.search(ctx, startOffset, filters, cb)
}

type ResultCollector struct {
	LimitSize int
	Rows      []Row
}

func (collector *ResultCollector) HandleRow(offset Offset, entry *Entry) error {
	collector.Rows = append(collector.Rows, Row{Offset: offset, Entry: entry})
	if len(collector.Rows) == collector.LimitSize {
		return SearchAborted
	}
	return nil
}
