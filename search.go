package lstore

import (
	"context"
	"io"
)

type SearchRequest struct {
	StartOffset   Offset
	BatchSizeHint int
	LimitSize     int
	Filters       []Filter
}

type blockIterator func() (block, error)
type RowIterator func() ([]Row, error)

// t1Search speed up by tree of bloomfilter (for int,blob) and min max (for int)
func t1Search(reader *Reader, filters []Filter) blockIterator {
	store := reader.currentVersion
	rawSegments := store.rawSegments
	currentRawSegmentIndex := 0
	return func() (block, error) {
		if currentRawSegmentIndex < len(rawSegments) {
			block := rawSegments[currentRawSegmentIndex].AsBlock
			currentRawSegmentIndex++
			return block, nil
		}
		if currentRawSegmentIndex == len(rawSegments) {
			currentRawSegmentIndex++
			return reader.tailBlock, nil
		}
		return nil, io.EOF
	}
}

// t2Search speed up by column based disk layout (for compacted segments)
// and in memory cache (for raw segments and tail segment)
func t2Search(reader *Reader, blkIter blockIterator, req SearchRequest) ([]Row, error) {
	var batch []Row
	for {
		blk, err := blkIter()
		if err != nil {
			if err == io.EOF && len(batch) > 0 {
				return batch, nil
			}
			return nil, err
		}
		batch, err = blk.search(reader, req.StartOffset, req.Filters, batch)
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
	blkIter := t1Search(reader, req.Filters)
	remaining := req.LimitSize
	return func() ([]Row, error) {
		batch, err := t2Search(reader, blkIter, req)
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
