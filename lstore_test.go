package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
)

func newTestStore() *Store {
	os.Remove("/tmp/lstore.bin")
	return NewStore("/tmp/lstore.bin")
}

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset, err := store.Write(0, Row{IntValues: []int64{1}})
	should.Nil(err)
	should.Equal(Offset(0x70), offset)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal(Row{IntValues: []int64{1}, FloatValues: []float64{}, BlobValues: []Blob{}}, row)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset1, err := store.Write(0, Row{IntValues: []int64{1}})
	should.Nil(err)
	_, err =  store.Write(offset1, Row{IntValues: []int64{2}})
	should.Nil(err)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal(int64(1), row.IntValues[0])
	row, err = store.Read(offset1)
	should.Nil(err)
	should.Equal(int64(2), row.IntValues[0])
}