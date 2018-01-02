package lstore

import (
	"io"
	"github.com/v2pro/plz/countlog"
)

type chunk interface {
	searchForward(ctx countlog.Context, startOffset Offset, filters []Filter, cb SearchCallback) error
}

type chunkIterator func() (chunk, error)

func iterateChunks(chunks []chunk) chunkIterator {
	i := 0
	return func() (chunk, error) {
		if i == len(chunks) {
			return nil, io.EOF
		}
		chunk := chunks[i]
		i++
		return chunk, nil
	}
}

func chainChunkIterator(chunkIterators ...chunkIterator) chunkIterator {
	i := 0
	return func() (chunk, error) {
		for {
			if i == len(chunkIterators) {
				return nil, io.EOF
			}
			iter := chunkIterators[i]
			chunk, err := iter()
			if err == io.EOF {
				i++
				continue
			} else if err != nil {
				return nil, err
			}
			return chunk, nil
		}
	}
}
