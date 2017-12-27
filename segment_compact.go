package lstore

type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint64

type compactSegment struct {
	SegmentHeader
	seqColumn    []RowSeq
	intColumns      []intColumn
	blobHashColumns []blobHashColumn
	blobColumns     []blobColumn
}

func (segment *compactSegment) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	mask := make([]bool, len(segment.seqColumn))
	for i, seq := range segment.seqColumn {
		if seq >= startSeq {
			mask[i] = true
		}
	}
	for _, filter := range filters {
		filter.updateMask(segment, mask)
	}
	var rows []Row
	for i, matches := range mask {
		if !matches {
			continue
		}
		intColumnsCount := len(segment.intColumns)
		intValues := make([]int64, intColumnsCount)
		for j := 0; j < intColumnsCount; j++ {
			intValues[j] = segment.intColumns[j][i]
		}
		blobColumnsCount := len(segment.blobColumns)
		blobValues := make([]Blob, blobColumnsCount)
		for j := 0; j < blobColumnsCount; j++ {
			blobValues[j] = segment.blobColumns[j][i]
		}
		rows = append(rows, Row{Seq: segment.seqColumn[i], Entry: &Entry{
			EntryType: EntryTypeData, IntValues: intValues, BlobValues: blobValues}})
	}
	return rows, nil
}