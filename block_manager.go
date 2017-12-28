package lstore

import (
	"github.com/spaolacci/murmur3"
	"unsafe"
	"github.com/hashicorp/golang-lru"
	"os"
	"sync"
	"github.com/v2pro/plz/countlog"
	"github.com/esdb/lstore/ref"
	"path"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/esdb/gocodec"
	"github.com/esdb/lstore/lz4"
)

type intColumn []int64
type blobColumn []Blob
type blobHashColumn []uint32

type compressedBlockHeader struct {
	originalSize   uint32
	compressedSize uint32
}
type block struct {
	seqColumn       []RowSeq
	intColumns      []intColumn
	blobHashColumns []blobHashColumn
	blobColumns     []blobColumn
}

// blockManager is global per store
// it manages the read/write to block file
// compress/decompress block from the mmap
// retain lru block cache
type blockManager struct {
	blockDirectory            string
	blockFileSizeInPowerOfTwo uint8 // 2 ^ x
	blockCache                *lru.ARCCache
	// tmp assume there is single writer
	tmp []byte
	// only lock the modification of following maps
	// does not cover reading or writing
	mapMutex   *sync.Mutex
	files      map[BlockSeq]*os.File
	writeMMaps map[BlockSeq]mmap.MMap
	readMMaps  map[BlockSeq]mmap.MMap
}

func newBlockManager(blockDirectory string, blockFileSizeInPowerOfTwo uint8) *blockManager {
	blockCache, _ := lru.NewARC(1024)
	return &blockManager{
		blockDirectory:            blockDirectory,
		blockFileSizeInPowerOfTwo: blockFileSizeInPowerOfTwo,
		blockCache:                blockCache,
		mapMutex:                  &sync.Mutex{},
		files:                     map[BlockSeq]*os.File{},
		readMMaps:                 map[BlockSeq]mmap.MMap{},
		writeMMaps:                map[BlockSeq]mmap.MMap{},
	}
}

func (mgr *blockManager) Close() error {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	var errs []error
	for _, writeMMap := range mgr.writeMMaps {
		err := writeMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!block_manager.failed to close writeMMap", "err", err)
		}
	}
	for _, readMMap := range mgr.readMMaps {
		err := readMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!block_manager.failed to close readMMap", "err", err)
		}
	}
	for _, file := range mgr.files {
		err := file.Close()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!block_manager.failed to close file", "err", err)
		}
	}
	return ref.NewMultiError(errs)
}

// TODO: handle wrap around
func (mgr *blockManager) writeBlock(blockSeq BlockSeq, block *block) (uint64, error) {
	fileBlockSeq := blockSeq >> mgr.blockFileSizeInPowerOfTwo
	relativeOffset := int(blockSeq - (fileBlockSeq << mgr.blockFileSizeInPowerOfTwo))
	writeMMap, err := mgr.openWriteMMap(fileBlockSeq)
	if err != nil {
		return 0, err
	}
	stream := gocodec.NewStream(nil)
	stream.Marshal(*block)
	if stream.Error != nil {
		return 0, stream.Error
	}
	buf := stream.Buffer()
	compressBound := lz4.CompressBound(len(buf))
	if compressBound > len(mgr.tmp) {
		mgr.tmp = make([]byte, compressBound)
	}
	compressedSize := lz4.CompressDefault(buf, mgr.tmp)
	stream.Reset(nil)
	blkSize := stream.Marshal(compressedBlockHeader{
		originalSize:  uint32(len(buf)),
		compressedSize: uint32(compressedSize),
	})
	if stream.Error != nil {
		return 0, stream.Error
	}
	copy(writeMMap[relativeOffset:], stream.Buffer())
	copy(writeMMap[relativeOffset+len(stream.Buffer()):], mgr.tmp[:compressedSize])
	err = writeMMap.Flush()
	if err != nil {
		return 0, err
	}
	return blkSize, nil
}

func (mgr *blockManager) readBlock(blockSeq BlockSeq) (*block, error) {
	fileBlockSeq := blockSeq >> mgr.blockFileSizeInPowerOfTwo
	relativeOffset := blockSeq - (fileBlockSeq << mgr.blockFileSizeInPowerOfTwo)
	readMMap, err := mgr.openReadMMap(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	iter := gocodec.NewIterator(readMMap[relativeOffset:])
	compressedBlockHeader, _ := iter.Unmarshal((*compressedBlockHeader)(nil)).(*compressedBlockHeader)
	if iter.Error != nil {
		return nil, iter.Error
	}
	decompressed := make([]byte, compressedBlockHeader.originalSize)
	compressed := iter.Buffer()[:compressedBlockHeader.compressedSize]
	lz4.DecompressSafe(compressed, decompressed)
	iter.Reset(decompressed)
	blk, _ := iter.Unmarshal((*block)(nil)).(*block)
	if iter.Error != nil {
		return nil, iter.Error
	}
	return blk, nil
}

func (mgr *blockManager) openReadMMap(fileBlockSeq BlockSeq) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	readMMap, err := mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		return nil, err
	}
	mgr.readMMaps[fileBlockSeq] = readMMap
	return readMMap, nil
}

func (mgr *blockManager) openWriteMMap(fileBlockSeq BlockSeq) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	writeMMap, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	mgr.writeMMaps[fileBlockSeq] = writeMMap
	return writeMMap, nil
}

func (mgr *blockManager) openFile(fileBlockSeq BlockSeq) (*os.File, error) {
	file := mgr.files[fileBlockSeq]
	if file != nil {
		return file, nil
	}
	filePath := path.Join(mgr.blockDirectory, fmt.Sprintf(
		"%d.block", fileBlockSeq<<mgr.blockFileSizeInPowerOfTwo))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		err = file.Truncate(2 << mgr.blockFileSizeInPowerOfTwo)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	mgr.files[fileBlockSeq] = file
	return file, nil
}

func newBlock(rows []Row) *block {
	rowsCount := len(rows)
	seqColumn := make([]RowSeq, rowsCount)
	intColumnsCount := len(rows[0].IntValues)
	intColumns := make([]intColumn, intColumnsCount)
	blobColumnsCount := len(rows[0].BlobValues)
	blobColumns := make([]blobColumn, blobColumnsCount)
	blobHashColumns := make([]blobHashColumn, blobColumnsCount)
	for i := 0; i < intColumnsCount; i++ {
		intColumns[i] = make(intColumn, rowsCount)
	}
	for i := 0; i < blobColumnsCount; i++ {
		blobColumns[i] = make(blobColumn, rowsCount)
		blobHashColumns[i] = make(blobHashColumn, rowsCount)
	}
	hasher := murmur3.New32()
	for i, row := range rows {
		seqColumn[i] = row.Seq
		for j, intValue := range row.IntValues {
			intColumns[j][i] = intValue
		}
		for j, blobValue := range row.BlobValues {
			blobColumns[j][i] = blobValue
			asSlice := *(*[]byte)(unsafe.Pointer(&blobValue))
			hasher.Reset()
			hasher.Write(asSlice)
			blobHashColumns[j][i] = hasher.Sum32()
		}
	}
	return &block{
		seqColumn:       seqColumn,
		intColumns:      intColumns,
		blobColumns:     blobColumns,
		blobHashColumns: blobHashColumns,
	}
}
