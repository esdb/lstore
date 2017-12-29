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

type blockIterator func() (chunk, error)
type RowIterator func() ([]Row, error)

const (
	iteratingCompacting = 1
	iteratingRaw        = 2
	iteratingTail       = 3
	iteratingDone       = 4
)

// t1Search speed up by tree of bloomfilter (for int,blob) and min max (for int)
func t1Search(reader *Reader, filters []Filter) blockIterator {
	store := reader.currentVersion
	rawSegments := store.rawSegments
	currentRawSegmentIndex := 0
	state := iteratingCompacting
	return func() (chunk, error) {
		switch state {
		case iteratingCompacting:
			goto iteratingCompacting
		case iteratingRaw:
			goto iteratingRaw
		case iteratingTail:
			goto iteratingTail
		case iteratingDone:
			return nil, io.EOF
		}
	iteratingCompacting:
		if store.compactingSegment != nil {
			state = iteratingRaw
			blk, _ := reader.store.blockManager.readBlock(0)
			return blk, nil
		}
		state = iteratingRaw
	iteratingRaw:
		if currentRawSegmentIndex < len(rawSegments) {
			segment := rawSegments[currentRawSegmentIndex].rows
			currentRawSegmentIndex++
			return segment, nil
		}
		state = iteratingTail
	iteratingTail:
		state = iteratingDone
		return reader.tailRows, nil
	}
}

// t2Search speed up by column based disk layout (for compacted segments)
// and in memory cache (for raw segments and tail chunk)
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
		batch, err = blk.search(reader, req.StartSeq, req.Filters, batch)
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
