package lstore

import (
	"path"
)

const tombstoneSegmentFileName = "tombstone.segment"
const tombstoneSegmentTmpFileName = "tombstone.segment.tmp"

type removerConfig struct {
	TombstoneSegmentDirectory string
}

func (conf *removerConfig) TombstoneSegmentPath() string {
	return path.Join(conf.TombstoneSegmentDirectory, tombstoneSegmentFileName)
}

func (conf *removerConfig) TombstoneSegmentTmpPath() string {
	return path.Join(conf.TombstoneSegmentDirectory, tombstoneSegmentTmpFileName)
}
