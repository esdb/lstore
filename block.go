package lstore

import (
	"unsafe"
	"github.com/esdb/pbloom"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/biter"
)

// the sequence number for compressed block
// the block reference in memory is called CompactSegment
type blockSeq uint64
type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint32

var blockLength = 256
var blockLengthInPowerOfTwo = uint8(8)

type block struct {
	startOffset Offset
	intColumns  []intColumn
	blobColumns []blobColumn
	// to speed up blob column linear search
	blobHashColumns []blobHashColumn
}

type hashColumn []pbloom.HashedElement

// to speed up computation of bloom filter
type blockHash []hashColumn

func newBlock(startOffset Offset, rows []*Entry) *block {
	rowsCount := blockLength
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
		for j, intValue := range row.IntValues {
			intColumns[j][i] = intValue
		}
		for j, blobValue := range row.BlobValues {
			blobColumns[j][i] = blobValue
		}
	}
	return &block{
		startOffset: startOffset,
		intColumns:  intColumns,
		blobColumns: blobColumns,
	}
}

func (blk *block) Hash(strategy *IndexingStrategy) blockHash {
	blockHash := make(blockHash, strategy.bloomFilterIndexedColumnsCount())
	for i := 0; i < len(blockHash); i++ {
		blockHash[i] = make(hashColumn, blockLength)
	}
	blobHashColumns := make([]blobHashColumn, len(strategy.bloomFilterIndexedBlobColumns))
	for i := 0; i < len(blobHashColumns); i++ {
		blobHashColumns[i] = make(blobHashColumn, blockLength)
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
			blobHashColumns[j][i] = hashed[0]
		}
	}
	blk.blobHashColumns = blobHashColumns
	return blockHash
}

func (blk *block) scanForward(ctx countlog.Context, startOffset Offset, filter Filter, cb SearchCallback) error {
	for _, beginSlot := range [4]biter.Slot{0, 64, 128, 192} {
		mask := biter.SetAllBits
		beginOffset := blk.startOffset + Offset(beginSlot)
		endOffset := beginOffset + Offset(64)
		if startOffset >= endOffset {
			continue
		}
		if startOffset > beginOffset {
			mask = biter.SetBitsForwardFrom[startOffset-beginOffset]
		}
		mask &= filter.searchBlock(blk, beginSlot)
		iter := mask.ScanForward()
		for {
			i := iter()
			if i == biter.NotFound {
				break
			}
			currentSlot := i + beginSlot
			if !filter.matchesBlockSlot(blk, currentSlot) {
				continue
			}
			intColumnsCount := len(blk.intColumns)
			intValues := make([]int64, intColumnsCount)
			for j := 0; j < intColumnsCount; j++ {
				intValues[j] = blk.intColumns[j][currentSlot]
			}
			blobColumnsCount := len(blk.blobColumns)
			blobValues := make([]Blob, blobColumnsCount)
			for j := 0; j < blobColumnsCount; j++ {
				blobValues[j] = blk.blobColumns[j][currentSlot]
			}
			err := cb.HandleRow(blk.startOffset+Offset(currentSlot), &Entry{
				EntryType: EntryTypeData, IntValues: intValues, BlobValues: blobValues})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
