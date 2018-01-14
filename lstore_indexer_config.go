package lstore

import (
	"time"
	"path"
	"fmt"
)

const IndexingSegmentFileName = "indexing.segment"
const IndexingSegmentTmpFileName = "indexing.segment.tmp"

type indexerConfig struct {
	IndexSegmentDirectory       string
	UpdateIndexInterval         time.Duration
	IndexSegmentMaxEntriesCount int
}

func (conf *indexerConfig) IndexedSegmentPath(tailOffset Offset) string {
	return path.Join(conf.IndexSegmentDirectory, fmt.Sprintf("indexed-%d.segment", tailOffset))
}

func (conf *indexerConfig) IndexingSegmentPath() string {
	return path.Join(conf.IndexSegmentDirectory, IndexingSegmentFileName)
}

func (conf *indexerConfig) IndexingSegmentTmpPath() string {
	return path.Join(conf.IndexSegmentDirectory, IndexingSegmentTmpFileName)
}
