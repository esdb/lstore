package lstore

import "github.com/esdb/gocodec"

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
	reader := &Reader{store: store, tailBlock: &rowBasedBlock{rows: nil}, gocIter: gocodec.NewIterator(nil)}
	if err := reader.Refresh(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (reader *Reader) Refresh() error {
	latestVersion := reader.store.Latest()
	if reader.currentVersion == nil || latestVersion.tailSegment != reader.currentVersion.TailSegment() {
		reader.currentVersion = latestVersion
		reader.tailRows = make([]Row, 0, 4)
		reader.tailOffset = reader.currentVersion.tailSegment.StartOffset
	}
	newRows, tailOffset, err := reader.currentVersion.tailSegment.read(reader.tailOffset, reader)
	if err != nil {
		return err
	}
	if len(newRows) == 0 {
		return nil
	}
	reader.tailRows = append(reader.tailRows, newRows...)
	reader.tailOffset = tailOffset
	reader.tailBlock = &rowBasedBlock{rows: reader.tailRows}
	return nil
}

func (reader *Reader) CurrentVersion() *StoreVersion {
	return reader.currentVersion
}

func (reader *Reader) TailBlock() Block {
	return reader.tailBlock
}

func (reader *Reader) GocIterator() *gocodec.Iterator {
	return reader.gocIter
}

func (reader *Reader) Close() error {
	return reader.currentVersion.Close()
}