package lstore

type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint64

type columnBasedBlock struct {
	offsetColumn    []Offset
	intColumns      []intColumn
	blobHashColumns []blobHashColumn
	blobColumns     []blobColumn
}

func (blk *columnBasedBlock) search(reader *Reader, startOffset Offset, filters []Filter, collector []Row) ([]Row, error) {
	mask := make([]bool, len(blk.offsetColumn))
	for i, offset := range blk.offsetColumn {
		if offset >= startOffset {
			mask[i] = true
		}
	}
	for _, filter := range filters {
		filter.updateMask(blk, mask)
	}
	var rows []Row
	for i, matches := range mask {
		if !matches {
			continue
		}
		intColumnsCount := len(blk.intColumns)
		intValues := make([]int64, intColumnsCount)
		for j := 0; j < intColumnsCount; j++ {
			intValues[j] = blk.intColumns[j][i]
		}
		blobColumnsCount := len(blk.blobColumns)
		blobValues := make([]Blob, blobColumnsCount)
		for j := 0; j < blobColumnsCount; j++ {
			blobValues[j] = blk.blobColumns[j][i]
		}
		rows = append(rows, Row{Offset: blk.offsetColumn[i], Entry: &Entry{
			EntryType: EntryTypeData, IntValues: intValues, BlobValues: blobValues}})
	}
	return rows, nil
}
