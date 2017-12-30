package lstore

import (
	"unsafe"
	"github.com/esdb/pbloom"
	"fmt"
)

// the sequence number for compressed block
// the block reference in memory is called CompactSegment
type BlockSeq uint64
type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint32

type block struct {
	seqColumn   []RowSeq
	intColumns  []intColumn
	blobColumns []blobColumn
	// to speed up blob column linear search
	blobHashColumns []blobHashColumn
}

type hashColumn []pbloom.HashedElement

// to speed up computation of bloom filter
type blockHash []hashColumn

type indexingStrategyConfig struct {
	BloomFilterIndexedIntColumns  []int
	BloomFilterIndexedBlobColumns []int
	MinMaxIndexedColumns          []int
}

type bloomFilterIndexedColumn [2]int

func (col bloomFilterIndexedColumn) IndexedColumn() int {
	return col[0]
}

func (col bloomFilterIndexedColumn) SourceColumn() int {
	return col[1]
}

type indexingStrategy struct {
	bigHashingStrategy            *pbloom.HashingStrategy
	smallHashingStrategy          *pbloom.HashingStrategy
	bloomFilterIndexedIntColumns  []bloomFilterIndexedColumn
	bloomFilterIndexedBlobColumns []bloomFilterIndexedColumn
	minMaxIndexedColumns          []int
}

func newIndexingStrategy(config *indexingStrategyConfig) *indexingStrategy {
	bloomFilterIndexedBlobColumns := make([]bloomFilterIndexedColumn, len(config.BloomFilterIndexedBlobColumns))
	for i := 0; i < len(bloomFilterIndexedBlobColumns); i++ {
		bloomFilterIndexedBlobColumns[i] = bloomFilterIndexedColumn{
			i,
			config.BloomFilterIndexedBlobColumns[i]}
	}
	bloomFilterIndexedIntColumns := make([]bloomFilterIndexedColumn, len(config.BloomFilterIndexedIntColumns))
	for i := 0; i < len(bloomFilterIndexedIntColumns); i++ {
		bloomFilterIndexedIntColumns[i] = bloomFilterIndexedColumn{
			len(bloomFilterIndexedBlobColumns) + i,
			config.BloomFilterIndexedIntColumns[i]}
	}
	bigHashingStrategy := pbloom.NewHashingStrategy(
		pbloom.HasherFnv, 157042, 7)
	smallHashingStrategy := pbloom.NewHashingStrategy(
		pbloom.HasherFnv, 2454, 7)
	return &indexingStrategy{
		smallHashingStrategy:          smallHashingStrategy,
		bigHashingStrategy:            bigHashingStrategy,
		bloomFilterIndexedIntColumns:  bloomFilterIndexedIntColumns,
		bloomFilterIndexedBlobColumns: bloomFilterIndexedBlobColumns,
		minMaxIndexedColumns:          config.MinMaxIndexedColumns,
	}
}

func (strategy *indexingStrategy) bloomFilterIndexedColumnsCount() int {
	return len(strategy.bloomFilterIndexedIntColumns) + len(strategy.bloomFilterIndexedBlobColumns)
}

func (strategy *indexingStrategy) lookupBlobColumn(sourceColumn int) int {
	for _, c := range strategy.bloomFilterIndexedBlobColumns {
		if c.SourceColumn() == sourceColumn {
			return c.IndexedColumn()
		}
	}
	panic(fmt.Sprintf("blob column not indexed: %d", sourceColumn))
}

func (strategy *indexingStrategy) lookupIntColumn(sourceColumn int) int {
	for _, c := range strategy.bloomFilterIndexedIntColumns {
		if c.SourceColumn() == sourceColumn {
			return c.IndexedColumn()
		}
	}
	panic(fmt.Sprintf("int column not indexed: %d", sourceColumn))
}

func newBlock(rows []Row) *block {
	rowsCount := len(rows)
	seqColumn := make([]RowSeq, rowsCount)
	intColumnsCount := len(rows[0].IntValues)
	intColumns := make([]intColumn, intColumnsCount)
	for i := 0; i < intColumnsCount; i++ {
		intColumns[i] = make(intColumn, rowsCount)
	}
	blobColumnsCount := len(rows[0].BlobValues)
	blobColumns := make([]blobColumn, blobColumnsCount)
	for i := 0; i < blobColumnsCount; i++ {
		blobColumns[i] = make(blobColumn, rowsCount)
	}
	for i, row := range rows {
		seqColumn[i] = row.Seq
		for j, intValue := range row.IntValues {
			intColumns[j][i] = intValue
		}
		for j, blobValue := range row.BlobValues {
			blobColumns[j][i] = blobValue
		}
	}
	return &block{
		seqColumn:   seqColumn,
		intColumns:  intColumns,
		blobColumns: blobColumns,
	}
}

func (blk *block) Hash(strategy *indexingStrategy) blockHash {
	rowsCount := len(blk.seqColumn)
	blockHash := make(blockHash, strategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(blockHash); i++ {
		blockHash[i] = make(hashColumn, rowsCount)
	}
	blobHashColumns := make([]blobHashColumn, len(strategy.bloomFilterIndexedBlobColumns))
	for i := 0; i < len(blobHashColumns); i++ {
		blobHashColumns[i] = make(blobHashColumn, rowsCount)
	}
	for _, bfIndexedColumn := range strategy.bloomFilterIndexedIntColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		for i, sourceValue := range blk.intColumns[sourceColumn] {
			asSlice := (*(*[8]byte)(unsafe.Pointer(&sourceValue)))[:]
			blockHash[indexedColumn][i] = strategy.smallHashingStrategy.HashStage1(asSlice)
		}
	}
	for j, bfIndexedColumn := range strategy.bloomFilterIndexedBlobColumns {
		indexedColumn := bfIndexedColumn.IndexedColumn()
		sourceColumn := bfIndexedColumn.SourceColumn()
		for i, sourceValue := range blk.blobColumns[sourceColumn] {
			asSlice := *(*[]byte)(unsafe.Pointer(&sourceValue))
			hashed := strategy.smallHashingStrategy.HashStage1(asSlice)
			blockHash[indexedColumn][i] = hashed
			blobHashColumns[j][i] = hashed.DownCastToUint32()
		}
	}
	blk.blobHashColumns = blobHashColumns
	return blockHash
}

func (blk *block) search(reader *Reader, startSeq RowSeq, filters []Filter, collector []Row) ([]Row, error) {
	mask := make([]bool, len(blk.seqColumn))
	for i, seq := range blk.seqColumn {
		if seq >= startSeq {
			mask[i] = true
		}
	}
	for _, filter := range filters {
		filter.searchBlock(blk, mask)
	}
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
		collector = append(collector, Row{Seq: blk.seqColumn[i], Entry: &Entry{
			EntryType: EntryTypeData, IntValues: intValues, BlobValues: blobValues}})
	}
	return collector, nil
}
