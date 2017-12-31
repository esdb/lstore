package lstore

import (
	"github.com/esdb/gocodec"
	"context"
	"io"
)

// Reader is not thread safe, can only be used from one goroutine
type Reader struct {
	store          *Store
	currentVersion *StoreVersion
	tailSeq        uint64 // seq to start next cache fill
	tailOffset     Offset
	tailRows       rowsChunk // rows cache
	gocIter        *gocodec.Iterator
}

type SearchRequest struct {
	StartOffset   Offset
	BatchSizeHint int
	LimitSize     int
	Filters       []Filter
}

type RowIterator func() ([]Row, error)

func (store *Store) NewReader() (*Reader, error) {
	reader := &Reader{
		store:    store,
		tailRows: rowsChunk{},
		gocIter:  gocodec.NewIterator(nil),
	}
	if _, err := reader.Refresh(); err != nil {
		return nil, err
	}
	return reader, nil
}

// Refresh has minimum cost of two cas read, one for store.latestVersion, one for tailSegment.tail
func (reader *Reader) Refresh() (bool, error) {
	latestVersion := reader.store.latest()
	defer latestVersion.Close()
	if reader.currentVersion == nil || latestVersion.tailSegment != reader.currentVersion.tailSegment {
		reader.tailSeq = 0
		reader.tailRows = make(rowsChunk, 0, blockLength)
	}
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
	return reader.currentVersion.tailSegment.read(reader)
}

func (reader *Reader) Close() error {
	return reader.currentVersion.Close()
}

func (reader *Reader) Search(ctx context.Context, req SearchRequest) RowIterator {
	if req.BatchSizeHint == 0 {
		req.BatchSizeHint = 64
	}
	chunkIter := scanForward(reader, req.Filters)
	remaining := req.LimitSize
	return func() ([]Row, error) {
		batch, err := searchChunks(reader, chunkIter, req)
		if err != nil {
			return nil, err
		}
		if req.LimitSize > 0 {
			if remaining == 0 {
				return nil, io.EOF
			}
			if remaining < len(batch) {
				batch = batch[:remaining]
			}
			remaining -= len(batch)
		}
		return batch, nil
	}
}

// scanForward speed up by tree of bloomfilter (for int,blob) and min max (for int)
func scanForward(reader *Reader, filters []Filter) chunkIterator {
	store := reader.currentVersion
	blockManager := reader.store.blockManager
	iter1 := store.indexedSegment.scanForward(blockManager, filters)
	var chunks []chunk
	for _, rawSegment := range store.rawSegments {
		chunks = append(chunks, rawSegment.rows)
	}
	chunks = append(chunks, reader.tailRows)
	iter2 := iterateChunks(chunks)
	return chainChunkIterator(iter1, iter2)
}

// searchChunks speed up by column based disk layout (for compacted segments)
// and in memory cache (for raw segments and tail segment)
func searchChunks(reader *Reader, chunkIter chunkIterator, req SearchRequest) ([]Row, error) {
	var batch []Row
	for {
		chunk, err := chunkIter()
		if err != nil {
			if err == io.EOF && len(batch) > 0 {
				return batch, nil
			}
			return nil, err
		}
		batch, err = chunk.search(reader, req.StartOffset, req.Filters, batch)
		if err != nil {
			return nil, err
		}
		if len(batch) >= req.BatchSizeHint {
			return batch, err
		}
	}
}
