package mheap

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/esdb/gocodec"
)

func Test_allocate(t *testing.T) {
	should := require.New(t)
	mgr := New(8, 2)
	allocated := mgr.Allocate(7, []byte{1, 2, 3})
	should.Equal([]byte{1, 2, 3}, allocated)
}

func Test_allocate_new_page(t *testing.T) {
	should := require.New(t)
	mgr := New(2, 2)
	allocated := mgr.Allocate(7, []byte{1, 2, 3})
	should.Equal([]byte{1, 2, 3}, allocated)
	allocated = mgr.Allocate(7, []byte{1, 2, 3})
	should.Equal([]byte{1, 2, 3}, allocated)
	should.Equal(1, len(mgr.oldPages))
}

func Test_expire_old_page(t *testing.T) {
	should := require.New(t)
	mgr := New(2, 1)
	allocated := mgr.Allocate(7, []byte{1, 2, 3})
	should.Equal([]byte{1, 2, 3}, allocated)
	allocated = mgr.Allocate(7, []byte{1, 2, 3})
	should.Equal([]byte{1, 2, 3}, allocated)
	should.Equal(0, len(mgr.oldPages))
}

func Test_goc_unmarshal_with_allocator(t *testing.T) {
	should := require.New(t)
	stream := gocodec.NewStream(nil)
	stream.Marshal([]uint16{1, 2, 3})
	buf := stream.Buffer()
	should.Equal([]byte{
		0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x1, 0x0, 0x2, 0x0, 0x3, 0x0,
	}, buf[16:])
	iter := gocodec.ReadonlyConfig.NewIterator(buf)
	mgr := New(10, 2)
	iter.Allocator(mgr)
	iter.ObjectSeq(7)
	obj := iter.Unmarshal((*[]uint16)(nil)).(*[]uint16)
	should.Equal([]uint16{1, 2, 3}, *obj)
	should.Equal(gocodec.ObjectSeq(7), mgr.lastPageRef)
	should.Equal([]gocodec.ObjectSeq{7}, mgr.lastPageRefs)
}

func Test_expire_page_should_invalid_cache(t *testing.T) {
	should := require.New(t)
	stream := gocodec.NewStream(nil)
	stream.Marshal([]uint16{1, 2, 3})
	buf := stream.Buffer()
	mgr := New(6, 1)
	obj, err := mgr.Unmarshal(7, buf, (*[]uint16)(nil))
	should.NoError(err)
	should.Equal([]uint16{1, 2, 3}, *obj.(*[]uint16))
	obj, err = mgr.Unmarshal(7, buf, (*[]uint16)(nil))
	should.NoError(err)
	should.Equal([]uint16{1, 2, 3}, *obj.(*[]uint16))
	should.NotNil(mgr.objects[7])
	obj, err = mgr.Unmarshal(8, buf, (*[]uint16)(nil))
	obj, err = mgr.Unmarshal(9, buf, (*[]uint16)(nil))
	should.Nil(mgr.objects[7])
}
