package lstore

import (
	"testing"
)

func Benchmark_column_based_block_scan(b *testing.B) {
	filters := []Filter{
		&IntValueFilter{Index: 0, Value: 100},
	}
	columnSize := 256
	blk := &columnBasedBlock{
		offsetColumn: make([]Offset, columnSize),
		intColumns: []intColumn{make(intColumn, columnSize)},
		blobHashColumns: []blobHashColumn{make(blobHashColumn, columnSize)},
		blobColumns: []blobColumn{make(blobColumn, columnSize)},
	}
	for i := 0; i < b.N; i++ {
		blk.search(nil, 0, filters, nil)
	}
}