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

func intDataRow(values ...int64) Row {
	return Row{RowType: RowTypeData, IntValues: values}
}

func blobDataRow(values ...Blob) Row {
	return Row{RowType: RowTypeData, BlobValues: values}
}

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset, err := store.Write(0, intDataRow(1))
	should.Nil(err)
	should.Equal(Offset(0x70), offset)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal([]int64{1}, row.IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset1, err := store.Write(0, intDataRow(1))
	should.Nil(err)
	_, err = store.Write(offset1, intDataRow(2))
	should.Nil(err)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal(int64(1), row.IntValues[0])
	row, err = store.Read(offset1)
	should.Nil(err)
	should.Equal(int64(2), row.IntValues[0])
}

func Test_append(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset1, err := store.Write(0, intDataRow(1))
	should.Nil(err)
	_, err = store.Append(intDataRow(2))
	should.Nil(err)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal(int64(1), row.IntValues[0])
	row, err = store.Read(offset1)
	should.Nil(err)
	should.Equal(int64(2), row.IntValues[0])
}

func Test_write_once(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Write(0, intDataRow(1))
	should.Nil(err)
	_, err = store.Write(0, intDataRow(1))
	should.Equal(WriteOnceError, err)
}

func Test_blob_value(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Append(blobDataRow(Blob("hello")))
	should.Nil(err)
	_, err = store.Append(blobDataRow(Blob("world")))
	should.Nil(err)
	_, err = store.Append(blobDataRow(Blob("hi")))
	should.Nil(err)
	rows, err :=store.ReadMultiple(0, 2)
	should.Nil(err)
	should.Equal("hello", string(rows[0].BlobValues[0]))
	should.Equal("world", string(rows[1].BlobValues[0]))
}

func Test_scan_by_blob(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Append(blobDataRow(Blob("hello")))
	should.Nil(err)
	_, err = store.Append(blobDataRow(Blob("world")))
	should.Nil(err)
	iter := store.Scan(0, 100, &BlobValueFilter{Value: Blob("world")})
	batch, err := iter()
	should.Nil(err)
	should.Equal(1, len(batch))
}
