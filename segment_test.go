package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
	"os"
)

func newTestSegment() *Segment {
	os.Remove("/tmp/lsegment.bin")
	segment, err := OpenSegment("/tmp/lsegment.bin", 200 * 1024)
	if err != nil {
		panic(err)
	}
	return segment
}

func intEntry(values ...int64) *Entry {
	return &Entry{EntryType: EntryTypeData, IntValues: values}
}

func blobEntry(values ...Blob) *Entry {
	return &Entry{EntryType: EntryTypeData, BlobValues: values}
}

func intBlobEntry(intValue int64, blobValue Blob) *Entry {
	return &Entry{EntryType: EntryTypeData, IntValues: []int64{intValue}, BlobValues: []Blob{blobValue}}
}

func Test_write_read_one_entry(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	offset, err := segment.Write(0, intEntry(1))
	should.Nil(err)
	should.Equal(Offset(0x70), offset)
	row, err := segment.Read(0)
	should.Nil(err)
	should.Equal([]int64{1}, row.IntValues)
}

func Test_write_two_entries(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	offset1, err := segment.Write(0, intEntry(1))
	should.Nil(err)
	_, err = segment.Write(offset1, intEntry(2))
	should.Nil(err)
	row, err := segment.Read(0)
	should.Nil(err)
	should.Equal(int64(1), row.IntValues[0])
	row, err = segment.Read(offset1)
	should.Nil(err)
	should.Equal(int64(2), row.IntValues[0])
}

func Test_append(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	offset1, err := segment.Write(0, intEntry(1))
	should.Nil(err)
	_, err = segment.Append(intEntry(2))
	should.Nil(err)
	row, err := segment.Read(0)
	should.Nil(err)
	should.Equal(int64(1), row.IntValues[0])
	row, err = segment.Read(offset1)
	should.Nil(err)
	should.Equal(int64(2), row.IntValues[0])
}

func Test_write_once(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	_, err := segment.Write(0, intEntry(1))
	should.Nil(err)
	_, err = segment.Write(0, intEntry(1))
	should.Equal(WriteOnceError, err)
}

func Test_blob_value(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	_, err := segment.Append(blobEntry(Blob("hello")))
	should.Nil(err)
	_, err = segment.Append(blobEntry(Blob("world")))
	should.Nil(err)
	_, err = segment.Append(blobEntry(Blob("hi")))
	should.Nil(err)
	rows, err := segment.ReadMultiple(0, 2)
	should.Nil(err)
	should.Equal("hello", string(rows[0].BlobValues[0]))
	should.Equal("world", string(rows[1].BlobValues[0]))
}

func Test_scan_by_blob(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	_, err := segment.Append(blobEntry(Blob("hello")))
	should.Nil(err)
	_, err = segment.Append(blobEntry(Blob("world")))
	should.Nil(err)
	iter := segment.Scan(0, 100, &BlobValueFilter{Value: Blob("world")})
	batch, err := iter()
	should.Nil(err)
	should.Equal(1, len(batch))
}

func Test_scan_by_int_range(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	_, err := segment.Append(intEntry(1))
	should.Nil(err)
	_, err = segment.Append(intEntry(3))
	should.Nil(err)
	_, err = segment.Append(intEntry(2))
	should.Nil(err)
	iter := segment.Scan(0, 100, &IntRangeFilter{Min: 1, Max: 2})
	rows, err := iter()
	should.Nil(err)
	should.Equal(2, len(rows))
	should.Equal(int64(1), rows[0].IntValues[0])
	should.Equal(int64(2), rows[1].IntValues[0])
}

func Test_scan_by_both_blob_and_int_range(t *testing.T) {
	should := require.New(t)
	segment := newTestSegment()
	_, err := segment.Append(intBlobEntry(1, Blob("hello")))
	should.Nil(err)
	_, err = segment.Append(intBlobEntry(3, Blob("world")))
	should.Nil(err)
	_, err = segment.Append(intBlobEntry(2, Blob("world")))
	should.Nil(err)
	iter := segment.Scan(0, 100, &AndFilter{[]Filter{
		&IntRangeFilter{Min: 1, Max: 2},
		&BlobValueFilter{Value: Blob("world")},
	}})
	rows, err := iter()
	should.Nil(err)
	should.Equal(1, len(rows))
	should.Equal(int64(2), rows[0].IntValues[0])
}

func Test_write_to_end_should_fail(t *testing.T) {
	should := require.New(t)
	os.Remove("/tmp/lsegment.bin")
	segment, err := OpenSegment("/tmp/lsegment.bin", 200)
	should.Nil(err)
	offset1, err := segment.Append(intEntry(1))
	should.Nil(err)
	segment.file.Truncate(int64(offset1))
	_, err = segment.Append(intEntry(1))
	should.Equal(SegmentOverflowError, err)
}
