package lstore

type segmentHeader struct {
	segmentType SegmentType
	startSeq    RowSeq
}

// 1 tail / multiple raw
const SegmentTypeRowBased SegmentType = 1

// 1 root indexed / 8 indexing
const SegmentTypeIndexing SegmentType = 2
const SegmentTypeIndexed SegmentType = 3