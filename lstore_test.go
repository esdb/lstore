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

func intRow(values ...int64) Row {
	return Row{RowType: RowTypeData, IntValues: values}
}

func blobRow(values ...Blob) Row {
	return Row{RowType: RowTypeData, BlobValues: values}
}

func intBlobRow(intValue int64, blobValue Blob) Row {
	return Row{RowType: RowTypeData, IntValues: []int64{intValue}, BlobValues: []Blob{blobValue}}
}

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset, err := store.Write(0, intRow(1))
	should.Nil(err)
	should.Equal(Offset(0x70), offset)
	row, err := store.Read(0)
	should.Nil(err)
	should.Equal([]int64{1}, row.IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	offset1, err := store.Write(0, intRow(1))
	should.Nil(err)
	_, err = store.Write(offset1, intRow(2))
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
	offset1, err := store.Write(0, intRow(1))
	should.Nil(err)
	_, err = store.Append(intRow(2))
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
	_, err := store.Write(0, intRow(1))
	should.Nil(err)
	_, err = store.Write(0, intRow(1))
	should.Equal(WriteOnceError, err)
}

func Test_blob_value(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Append(blobRow(Blob("hello")))
	should.Nil(err)
	_, err = store.Append(blobRow(Blob("world")))
	should.Nil(err)
	_, err = store.Append(blobRow(Blob("hi")))
	should.Nil(err)
	rows, err := store.ReadMultiple(0, 2)
	should.Nil(err)
	should.Equal("hello", string(rows[0].BlobValues[0]))
	should.Equal("world", string(rows[1].BlobValues[0]))
}

func Test_scan_by_blob(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Append(blobRow(Blob("hello")))
	should.Nil(err)
	_, err = store.Append(blobRow(Blob("world")))
	should.Nil(err)
	iter := store.Scan(0, 100, &BlobValueFilter{Value: Blob("world")})
	batch, err := iter()
	should.Nil(err)
	should.Equal(1, len(batch))
}

func Test_scan_by_int_range(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Append(intRow(1))
	should.Nil(err)
	_, err = store.Append(intRow(3))
	should.Nil(err)
	_, err = store.Append(intRow(2))
	should.Nil(err)
	iter := store.Scan(0, 100, &IntRangeFilter{Min: 1, Max: 2})
	rows, err := iter()
	should.Nil(err)
	should.Equal(2, len(rows))
	should.Equal(int64(1), rows[0].IntValues[0])
	should.Equal(int64(2), rows[1].IntValues[0])
}

func Test_scan_by_both_blob_and_int_range(t *testing.T) {
	should := require.New(t)
	store := newTestStore()
	_, err := store.Append(intBlobRow(1, Blob("hello")))
	should.Nil(err)
	_, err = store.Append(intBlobRow(3, Blob("world")))
	should.Nil(err)
	_, err = store.Append(intBlobRow(2, Blob("world")))
	should.Nil(err)
	iter := store.Scan(0, 100, &AndFilter{[]Filter{
		&IntRangeFilter{Min: 1, Max: 2},
		&BlobValueFilter{Value: Blob("world")},
	}})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal(int64(2), rows[0].IntValues[0])
}
