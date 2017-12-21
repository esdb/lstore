package lstore

import (
	"errors"
	"github.com/esdb/gocodec"
	"os"
	"github.com/edsrzf/mmap-go"
	"github.com/v2pro/plz/countlog"
)

type Store struct {
	mapped        mmap.MMap
	file          *os.File
	filename      string
	currentOffset Offset
	stream        *gocodec.Stream
	iter          *gocodec.Iterator
}

func NewStore(filename string) *Store {
	return &Store{
		filename:      filename,
		currentOffset: 0,
		stream:        gocodec.NewStream(nil),
		iter:          gocodec.NewIterator(nil),
	}
}

type Blob []byte
type RowType uint8

const RowTypeData RowType = 7
const RowTypeJunk RowType = 6
const RowTypeConfigurationChange = 5

type Row struct {
	Reserved    uint8
	RowType     RowType
	IntValues   []int64
	FloatValues []float64
	BlobValues  []Blob
}

type Offset uint64

func (store *Store) init() error {
	if store.file != nil {
		return nil
	}
	file, err := os.OpenFile(store.filename, os.O_RDWR, 0666)
	if err != nil {
		file, err = os.OpenFile(store.filename, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		err = file.Truncate(200 * 1024 * 1024)
		if err != nil {
			return err
		}
	}
	store.file = file
	mapped, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		countlog.Error("event!store.failed to mmap", "err", err, "filename", store.filename)
		return err
	}
	store.mapped = mapped
	return nil
}

func (store *Store) close() error {
	if store.mapped != nil {
		err := store.mapped.Unmap()
		if err != nil {
			return err
		}
	}
	if store.file != nil {
		err := store.file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) Write(offset Offset, row Row) (Offset, error) {
	err := store.init()
	if err != nil {
		return 0, err
	}
	if offset != store.currentOffset {
		return 0, errors.New("only support sequential write")
	}
	gocStream := store.stream
	gocStream.Reset(nil)
	gocStream.Marshal(row)
	if gocStream.Error != nil {
		return 0, gocStream.Error
	}
	buf := gocStream.Buffer()
	copy(store.mapped[store.currentOffset:], buf)
	writtenAt := store.currentOffset
	store.currentOffset += Offset(len(buf))
	return writtenAt, nil
}

func (store *Store) Read(offset Offset) (Row, error) {
	err := store.init()
	if err != nil {
		return Row{}, err
	}
	iter := store.iter
	iter.Reset(store.mapped[offset:])
	row := iter.Unmarshal((*Row)(nil))
	if iter.Error != nil {
		return Row{}, err
	}
	return *row.(*Row), nil
}
