package lstore

import (
	"fmt"
	"path"
	"time"
)

const indexingSegmentFileName = "indexing.segment"
const indexingSegmentTmpFileName = "indexing.segment.tmp"

type indexerConfig struct {
	IndexSegmentDirectory       string
	UpdateIndexInterval         time.Duration
	IndexSegmentMaxEntriesCount int
}

func (conf *indexerConfig) IndexedSegmentPath(tailOffset Offset) string {
	return path.Join(conf.IndexSegmentDirectory, fmt.Sprintf("indexed-%d.segment", tailOffset))
}

func (conf *indexerConfig) IndexingSegmentPath() string {
	return path.Join(conf.IndexSegmentDirectory, indexingSegmentFileName)
}

func (conf *indexerConfig) IndexingSegmentTmpPath() string {
	return path.Join(conf.IndexSegmentDirectory, indexingSegmentTmpFileName)
}
