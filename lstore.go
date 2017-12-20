package lstore

type Store struct {
	file string
}

func NewStore(file string) *Store {
	return &Store{file: file}
}

type Row []interface{}
type Offset uint64

func (store *Store) Write(offset Offset, row Row) (Offset, error) {
	return 0, nil
}