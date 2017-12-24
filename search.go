package lstore

import (
	"context"
	"github.com/esdb/lstore"
	"io"
)

type Request struct {
	StartOffset   lstore.Offset
	BatchSizeHint int
	LimitSize     int
	Filters       []lstore.Filter
}

type blockIterator func() (lstore.Block, error)
type RowIterator func() ([]lstore.Row, error)

// t1Search speed up by tree of bloomfilter (for int,blob) and min max (for int)
func t1Search(reader *lstore.Reader, filters []lstore.Filter) blockIterator {
	store := reader.CurrentVersion()
	rawSegments := store.RawSegments()
	currentRawSegmentIndex := 0
	return func() (lstore.Block, error) {
		if currentRawSegmentIndex < len(rawSegments) {
			block := rawSegments[currentRawSegmentIndex].AsBlock
			currentRawSegmentIndex++
			return block, nil
		}
		if currentRawSegmentIndex == len(rawSegments) {
			currentRawSegmentIndex++
			return reader.TailBlock(), nil
		}
		return nil, io.EOF
	}
}

// t2Search speed up by column based disk layout (for compacted segments)
// and in memory cache (for raw segments and tail segment)
func t2Search(reader *lstore.Reader, blkIter blockIterator, req Request) ([]lstore.Row, error) {

	var batch []lstore.Row
	for {
		blk, err := blkIter()
		if err != nil {
			if err == io.EOF && len(batch) > 0 {
				return batch, nil
			}
			return nil, err
		}
		batch, err = blk.Search(reader, req.StartOffset, req.Filters, batch)
		if err != nil {
			return nil, err
		}
		if len(batch) >= req.BatchSizeHint {
			return batch, err
		}
	}
}

func Execute(ctx context.Context, reader *lstore.Reader, req Request) RowIterator {
	if req.BatchSizeHint == 0 {
		req.BatchSizeHint = 64
	}
	blkIter := t1Search(reader, req.Filters)
	remaining := req.LimitSize
	return func() ([]lstore.Row, error) {
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
