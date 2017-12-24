package lstore

import (
	"github.com/esdb/gocodec"
)

// Reader is not thread safe, can only be used from one goroutine
type Reader struct {
	store          *Store
	currentVersion *StoreVersion
	tailRows       []Row // cache of tail segment
	tailOffset     Offset // offset to start next cache fill
	tailBlock      Block // expose search api for tail segment
	gocIter		   *gocodec.Iterator
}

func (store *Store) NewReader() (*Reader, error) {
	reader := &Reader{
		store: store,
		tailBlock: &rowBasedBlock{rows: nil},
		gocIter: gocodec.NewIterator(nil),
		currentVersion: store.Latest(),
	}
	if err := reader.Refresh(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (reader *Reader) Refresh() error {
	latestVersion := reader.store.Latest()
	if latestVersion.tailSegment != reader.currentVersion.TailSegment() {
		reader.tailRows = make([]Row, 0, 4)
		reader.tailOffset = latestVersion.tailSegment.StartOffset
		reader.tailBlock = nil
	}
	if reader.currentVersion != latestVersion {
		if err := reader.currentVersion.Close(); err != nil {
			return err
		}
		reader.currentVersion = latestVersion
	}
	return reader.currentVersion.tailSegment.read(reader)
}

func (reader *Reader) CurrentVersion() *StoreVersion {
	return reader.currentVersion
}

func (reader *Reader) TailBlock() Block {
	return reader.tailBlock
}

func (reader *Reader) Close() error {
	return reader.currentVersion.Close()
}