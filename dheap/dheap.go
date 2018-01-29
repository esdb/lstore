package dheap

import (
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz"
	"github.com/v2pro/plz/countlog"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"sync"
)

// DiskManager is thread safe
type DiskManager struct {
	// only lock the modification of following maps
	// does not cover reading or writing
	mapMutex             *sync.Mutex
	files                map[uint64]*os.File
	writeMMaps           map[uint64]mmap.MMap
	readMMaps            map[uint64]mmap.MMap
	directory            string
	fileSizeInPowerOfTwo uint8 // 2 ^ x
	fileSize             uint64
}

func New(directory string, fileSizeInPowerOfTwo uint8) *DiskManager {
	return &DiskManager{
		directory:            directory,
		fileSizeInPowerOfTwo: fileSizeInPowerOfTwo,
		fileSize:             1 << fileSizeInPowerOfTwo,
		mapMutex:             &sync.Mutex{},
		files:                map[uint64]*os.File{},
		readMMaps:            map[uint64]mmap.MMap{},
		writeMMaps:           map[uint64]mmap.MMap{},
	}
}

func (mgr *DiskManager) Close() error {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	var errs []error
	for _, writeMMap := range mgr.writeMMaps {
		err := writeMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DiskManager.failed to close writeMMap", "err", err)
		}
	}
	for _, readMMap := range mgr.readMMaps {
		err := readMMap.Unmap()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DiskManager.failed to close readMMap", "err", err)
		}
	}
	for _, file := range mgr.files {
		err := file.Close()
		if err != nil {
			errs = append(errs, err)
			countlog.Error("event!DiskManager.failed to close file", "err", err)
		}
	}
	return plz.MergeErrors(errs...)
}

func (mgr *DiskManager) WriteBuf(seq uint64, buf []byte) (uint64, error) {
	if seq == 0 {
		return 0, errors.New("sequence 0 is not a valid sequence number")
	}
	pageSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := int(seq - (pageSeq << mgr.fileSizeInPowerOfTwo))
	writeMMap, err := mgr.openWriteMMap(pageSeq)
	if err != nil {
		return 0, err
	}
	dst := writeMMap[relativeOffset:]
	if len(dst) < len(buf) {
		if uint64(len(buf)) > mgr.fileSize {
			countlog.Error("event!dheap.WriteBuf can not fulfill the request",
				"seq", seq, "relativeOffset", relativeOffset,
				"dstSize", len(dst), "bufSize", len(buf))
			return 0, errors.New("WriteBuf can not fulfill the request")
		}
		// write the buf to next file
		newSeq := seq + uint64(len(dst))
		countlog.Debug("event!dheap.write buf rotate to new file",
			"seq", seq, "newSeq", newSeq, "relativeOffset", relativeOffset,
			"dstSize", len(dst), "bufSize", len(buf))
		return mgr.WriteBuf(newSeq, buf)
	}
	copy(dst, buf)
	return seq, writeMMap.Flush()
}

func (mgr *DiskManager) AllocateBuf(seq uint64, size uint32) (uint64, []byte, error) {
	if seq == 0 {
		return 0, nil, errors.New("sequence 0 is not a valid sequence number")
	}
	pageSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := int(seq - (pageSeq << mgr.fileSizeInPowerOfTwo))
	writeMMap, err := mgr.openWriteMMap(pageSeq)
	if err != nil {
		return 0, nil, err
	}
	dst := writeMMap[relativeOffset:]
	if uint32(len(dst)) < size {
		if uint64(size) > mgr.fileSize {
			countlog.Error("event!dheap.AllocateBuf can not fulfill the request",
				"seq", seq, "relativeOffset", relativeOffset,
				"dstSize", len(dst), "bufSize", size)
			return 0, nil, errors.New("AllocateBuf can not fulfill the request")
		}
		// allocate the buf to next file
		newSeq := seq + uint64(len(dst))
		countlog.Debug("event!dheap.allocate buf rotate to new file",
			"seq", seq, "newSeq", newSeq, "relativeOffset", relativeOffset,
			"dstSize", len(dst), "bufSize", size)
		return mgr.AllocateBuf(newSeq, size)
	}
	return seq, dst[:size], nil
}

func (mgr *DiskManager) ReadBuf(seq uint64, size uint32) ([]byte, error) {
	if seq == 0 {
		return nil, errors.New("sequence 0 is not a valid sequence number")
	}
	pageSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := seq - (pageSeq << mgr.fileSizeInPowerOfTwo)
	readMMap, err := mgr.openReadMMap(pageSeq)
	if err != nil {
		return nil, err
	}
	buf := readMMap[relativeOffset:]
	if uint32(len(buf)) < size {
		panic("size overflow the data file")
	}
	return buf[:size], nil
}

func (mgr *DiskManager) MapWritableBuf(seq uint64, size uint32) ([]byte, error) {
	if seq == 0 {
		return nil, errors.New("sequence 0 is not a valid sequence number")
	}
	pageSeq := seq >> mgr.fileSizeInPowerOfTwo
	relativeOffset := seq - (pageSeq << mgr.fileSizeInPowerOfTwo)
	writeMMap, err := mgr.openWriteMMap(pageSeq)
	if err != nil {
		return nil, err
	}
	buf := writeMMap[relativeOffset:]
	if uint32(len(buf)) < size {
		panic("size overflow the data file")
	}
	return buf[:size], nil
}

func (mgr *DiskManager) flush(seq uint64) error {
	pageSeq := seq >> mgr.fileSizeInPowerOfTwo
	writeMMap, err := mgr.openWriteMMap(pageSeq)
	if err != nil {
		return err
	}
	return writeMMap.Flush()
}

func (mgr *DiskManager) openReadMMap(pageSeq uint64) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(pageSeq)
	if err != nil {
		return nil, err
	}
	readMMap := mgr.readMMaps[pageSeq]
	if readMMap != nil {
		return readMMap, nil
	}
	readMMap, err = mmap.Map(file, mmap.RDONLY, 0)
	countlog.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return nil, err
	}
	mgr.readMMaps[pageSeq] = readMMap
	return readMMap, nil
}

func (mgr *DiskManager) openWriteMMap(pageSeq uint64) (mmap.MMap, error) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	file, err := mgr.openFile(pageSeq)
	if err != nil {
		return nil, err
	}
	writeMMap := mgr.writeMMaps[pageSeq]
	if writeMMap != nil {
		return writeMMap, nil
	}
	writeMMap, err = mmap.Map(file, mmap.RDWR, 0)
	countlog.TraceCall("callee!mmap.Map", err)
	if err != nil {
		return nil, fmt.Errorf("map RDWR for block failed: %s", err.Error())
	}
	mgr.writeMMaps[pageSeq] = writeMMap
	return writeMMap, nil
}

func (mgr *DiskManager) openFile(pageSeq uint64) (*os.File, error) {
	file := mgr.files[pageSeq]
	if file != nil {
		return file, nil
	}
	filePath := path.Join(mgr.directory, strconv.Itoa(int(pageSeq)))
	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		os.MkdirAll(path.Dir(filePath), 0777)
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
		countlog.DebugCall("callee!os.OpenFile", err, "filePath", filePath, "fileSize", mgr.fileSize)
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
	mgr.files[pageSeq] = file
	return file, nil
}

func (mgr *DiskManager) Remove(untilSeq uint64) {
	mgr.mapMutex.Lock()
	defer mgr.mapMutex.Unlock()
	minFileName, err := getMinFileName(mgr.directory)
	countlog.TraceCall("callee!getMinFileName", err)
	if err != nil {
		return
	}
	pageSeq := untilSeq >> mgr.fileSizeInPowerOfTwo
	var resources []io.Closer
	countlog.Debug("event!dheap.remove files", "from", minFileName, "to", pageSeq)
	for i := minFileName; i < pageSeq; i++ {
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
		filePath := path.Join(mgr.directory, strconv.Itoa(int(i)))
		resources = append(resources, wrapFileRemover(filePath))
	}
	if len(resources) != 0 {
		go func() {
			plz.CloseAll(resources,
				"dataManager.removeUntil", untilSeq,
				"pageSeq", pageSeq)
		}()
	}
}

func getMinFileName(dir string) (uint64, error) {
	files, err := ioutil.ReadDir(dir)
	countlog.TraceCall("callee!ioutil.ReadDir", err)
	if err != nil {
		return 0, err
	}
	minFileName := uint64(math.MaxUint64)
	for _, file := range files {
		no, err := strconv.Atoi(file.Name())
		if err != nil {
			continue
		}
		if uint64(no) < minFileName {
			minFileName = uint64(no)
		}
	}
	return minFileName, nil
}

func wrapFileRemover(filePath string) io.Closer {
	return plz.WrapCloser(func() error {
		return os.Remove(filePath)
	})
}
