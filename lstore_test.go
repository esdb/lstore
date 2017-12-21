package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_write_read(t *testing.T) {
	should := require.New(t)
	store := NewStore("/tmp/lstore.bin")
	offset, err := store.Write(0, Row{IntValues: []int64{1}})
	should.Nil(err)
	should.Equal(Offset(0), offset)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal(Row{IntValues: []int64{1}, FloatValues: []float64{}, BlobValues: []Blob{}}, row)
}