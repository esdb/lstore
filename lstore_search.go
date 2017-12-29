package lstore

import (
	"context"
	"io"
)

type SearchRequest struct {
	StartSeq      RowSeq
	BatchSizeHint int
	LimitSize     int
	Filters       []Filter
}

type RowIterator func() ([]Row, error)

const (
	iteratingCompacting = 1
	iteratingRaw        = 2
	iteratingTail       = 3
	iteratingDone       = 4
)

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
		batch, err = chunk.search(reader, req.StartSeq, req.Filters, batch)
		if err != nil {
			return nil, err
		}
		if len(batch) >= req.BatchSizeHint {
			return batch, err
		}
	}
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
