package dheap

import (
	"sync"
	"github.com/edsrzf/mmap-go"
	"os"
	"github.com/v2pro/plz/countlog"
	"fmt"
	"path"
	"github.com/v2pro/plz"
	"math"
	"io"
)

type DataManager struct {
	// only lock the modification of following maps
	// does not cover reading or writing
	mapMutex              *sync.Mutex
	files                 map[uint64]*os.File
	writeMMaps            map[uint64]mmap.MMap
	readMMaps             map[uint64]mmap.MMap
	minOpenedFileBlockSeq uint64
	lockedSeqs            map[uint64]struct{}
	directory             string
	fileSizeInPowerOfTwo  uint8 // 2 ^ x
	fileSize              uint64
}

func New(directory string, fileSizeInPowerOfTwo uint8) *DataManager {
	lockedSeqs := make(map[uint64]struct{}, 100)
	return &DataManager{
		directory:             directory,
		fileSizeInPowerOfTwo:  fileSizeInPowerOfTwo,
		fileSize:              1 << fileSizeInPowerOfTwo,
		mapMutex:              &sync.Mutex{},
		lockedSeqs:            lockedSeqs,
		minOpenedFileBlockSeq: math.MaxUint64,
		files:                 map[uint64]*os.File{},
		readMMaps:             map[uint64]mmap.MMap{},
		writeMMaps:            map[uint64]mmap.MMap{},
	}
}

func (mgr *DataManager) Close() error {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	var errs []error
	for _, writeMMap := range mgr.writeMMaps {
		err := writeMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DataManager.failed to close writeMMap", "err", err)
		}
	}
	for _, readMMap := range mgr.readMMaps {
		err := readMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DataManager.failed to close readMMap", "err", err)
		}
	}
	for _, file := range mgr.files {
		err := file.Close()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DataManager.failed to close file", "err", err)
		}
	}
	return plz.MergeErrors(errs...)
}

func (mgr *DataManager) WriteBuf(seq uint64, buf []byte) (uint64, error) {
	fileBlockSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := int(seq - (fileBlockSeq << mgr.fileSizeInPowerOfTwo))
	writeMMap, err := mgr.openWriteMMap(fileBlockSeq)
	if err != nil {
		return 0, err
	}
	dst := writeMMap[relativeOffset:]
	if len(dst) < len(buf) {
		// write the buf to next file
		return mgr.WriteBuf(seq+uint64(len(dst)), buf)
	}
	copy(dst, buf)
	return seq, writeMMap.Flush()
}

func (mgr *DataManager) AllocateBuf(seq uint64, size uint32) (uint64, []byte, error) {
	fileBlockSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := int(seq - (fileBlockSeq << mgr.fileSizeInPowerOfTwo))
	writeMMap, err := mgr.openWriteMMap(fileBlockSeq)
	if err != nil {
		return 0, nil, err
	}
	dst := writeMMap[relativeOffset:]
	if uint32(len(dst)) < size {
		// allocate the buf to next file
		return mgr.AllocateBuf(seq+uint64(len(dst)), size)
	}
	return seq, dst[:size], nil
}

func (mgr *DataManager) ReadBuf(seq uint64, size uint32) ([]byte, error) {
	fileBlockSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := seq - (fileBlockSeq << mgr.fileSizeInPowerOfTwo)
	readMMap, err := mgr.openReadMMap(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	buf := readMMap[relativeOffset:]
	if uint32(len(buf)) < size {
		panic("size overflow the data file")
	}
	return buf[:size], nil
}

func (mgr *DataManager) MapWritableBuf(seq uint64, size uint32) ([]byte, error) {
	fileBlockSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := seq - (fileBlockSeq << mgr.fileSizeInPowerOfTwo)
	writeMMap, err := mgr.openWriteMMap(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	buf := writeMMap[relativeOffset:]
	if uint32(len(buf)) < size {
		panic("size overflow the data file")
	}
	return buf[:size], nil
}

func (mgr *DataManager) flush(seq uint64) (error) {
	fileBlockSeq := seq >> mgr.fileSizeInPowerOfTwo
	writeMMap, err := mgr.openWriteMMap(fileBlockSeq)
	if err != nil {
		return err
	}
	return writeMMap.Flush()
}

func (mgr *DataManager) openReadMMap(fileBlockSeq uint64) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	readMMap := mgr.readMMaps[fileBlockSeq]
	if readMMap != nil {
		return readMMap, nil
	}
	readMMap, err = mmap.Map(file, mmap.COPY, 0)
	if err != nil {
		return nil, err
	}
	mgr.readMMaps[fileBlockSeq] = readMMap
	return readMMap, nil
}

func (mgr *DataManager) openWriteMMap(fileBlockSeq uint64) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(fileBlockSeq)
	if err != nil {
		return nil, err
	}
	writeMMap := mgr.writeMMaps[fileBlockSeq]
	if writeMMap != nil {
		return writeMMap, nil
	}
	writeMMap, err = mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("map RDWR for block failed: %s", err.Error())
	}
	mgr.writeMMaps[fileBlockSeq] = writeMMap
	return writeMMap, nil
}

func (mgr *DataManager) openFile(fileBlockSeq uint64) (*os.File, error) {
	file := mgr.files[fileBlockSeq]
	if file != nil {
		return file, nil
	}
	if fileBlockSeq < mgr.minOpenedFileBlockSeq {
		mgr.minOpenedFileBlockSeq = fileBlockSeq
	}
	filePath := path.Join(mgr.directory, fmt.Sprintf(
		"%d.dat", fileBlockSeq))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		os.MkdirAll(path.Dir(filePath), 0777)
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		err = file.Truncate(int64(mgr.fileSize))
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	mgr.files[fileBlockSeq] = file
	return file, nil
}

func (mgr *DataManager) Lock(seq uint64) error {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	mgr.lockedSeqs[seq] = struct{}{}
	return nil
}

func (mgr *DataManager) Unlock(seq uint64) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	delete(mgr.lockedSeqs, seq)
}

func (mgr *DataManager) Remove(untilSeq uint64) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	minLockedSeq := uint64(math.MaxUint64)
	for lockedSeq := range mgr.lockedSeqs {
		if lockedSeq < minLockedSeq {
			minLockedSeq = lockedSeq
		}
	}
	fileBlockSeq := untilSeq >> mgr.fileSizeInPowerOfTwo
	minRemovable := minLockedSeq >> mgr.fileSizeInPowerOfTwo
	if minRemovable < fileBlockSeq {
		countlog.Debug("event!dheap.not all file blocks will be removed",
			"minRemovable", minRemovable,
			"fileBlockSeq", fileBlockSeq)
		fileBlockSeq = minRemovable
	}
	var resources []io.Closer
	if mgr.minOpenedFileBlockSeq >= fileBlockSeq {
		countlog.Debug("event!dheap.no file blocks will be removed",
			"minOpenedFileBlockSeq", mgr.minOpenedFileBlockSeq,
			"fileBlockSeq", fileBlockSeq)
		return
	}
	for i := mgr.minOpenedFileBlockSeq; i < fileBlockSeq; i++ {
		if readMMap := mgr.readMMaps[i]; readMMap != nil {
			resources = append(resources, plz.WrapCloser(readMMap.Unmap))
			delete(mgr.readMMaps, i)
		}
		if writeMMap := mgr.writeMMaps[i]; writeMMap != nil {
			resources = append(resources, plz.WrapCloser(writeMMap.Unmap))
			delete(mgr.writeMMaps, i)
		}
		if file := mgr.files[i]; file != nil {
			resources = append(resources, file)
			delete(mgr.files, i)
		}
		filePath := path.Join(mgr.directory, fmt.Sprintf(
			"%d.dat", i))
		resources = append(resources, wrapFileRemover(filePath))
	}
	mgr.minOpenedFileBlockSeq = fileBlockSeq
	if len(resources) != 0 {
		go func() {
			plz.CloseAll(resources,
				"dataManager.removeUntil", untilSeq,
				"fileBlockSeq", fileBlockSeq)
		}()
	}
}

func wrapFileRemover(filePath string) io.Closer {
	return plz.WrapCloser(func() error {
		return os.Remove(filePath)
	})
}
