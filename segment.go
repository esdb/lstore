package lstore

type segmentHeader struct {
	segmentType SegmentType
	startSeq    RowSeq
}

// 1 tail / multiple raw
const SegmentTypeRowBased SegmentType = 1

// 1 root indexed / 64 indexed
const SegmentTypeRootIndexed SegmentType = 2
const SegmentTypeIndexed SegmentType = 3

// 1 root compacted / 64 compacted
const SegmentTypeRootCompacted SegmentType = 4
const SegmentTypeCompacted SegmentType = 5

// 1 freezing / multiple frozen
const SegmentTypeFreezing SegmentType = 6
const SegmentTypeFrozen SegmentType = 7
