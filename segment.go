package lstore

type SegmentHeader struct {
	SegmentType SegmentType
	StartSeq RowSeq
}

const SegmentTypeRowBased SegmentType = 1
const SegmentTypeCompacting SegmentType = 2
const SegmentTypeCompactingFragment SegmentType = 3
const SegmentTypeCompacted SegmentType = 4