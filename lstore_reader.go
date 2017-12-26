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
	}
	if err := reader.Refresh(); err != nil {
		return nil, err
	}
	return reader, nil
}

// Refresh has minimum cost of two cas read, one for store.latestVersion, one for tailSegment.tail
func (reader *Reader) Refresh() error {
	latestVersion := reader.store.latest()
	if reader.currentVersion == nil || latestVersion.tailSegment != reader.currentVersion.tailSegment {
		reader.tailRows = make([]Row, 0, 4)
		reader.tailOffset = latestVersion.tailSegment.StartOffset
		reader.tailBlock = &rowBasedBlock{}
	}
	if reader.currentVersion != latestVersion {
		// when reader moves forward, older version has a chance to die
		if reader.currentVersion != nil {
			if err := reader.currentVersion.Close(); err != nil {
				return err
			}
		}
		reader.currentVersion = latestVersion
	}
	return reader.currentVersion.tailSegment.read(reader)
}

func (reader *Reader) Close() error {
	return reader.currentVersion.Close()
}