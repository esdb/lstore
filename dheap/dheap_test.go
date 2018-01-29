package dheap

import (
	"github.com/stretchr/testify/require"
	"github.com/v2pro/plz"
	"os"
	"testing"
	"time"
)

func Test_read_write_buf(t *testing.T) {
	should := require.New(t)
	os.RemoveAll("/tmp/store")
	os.Mkdir("/tmp/store", 0777)
	mgr := New("/tmp/store", 4)
	defer plz.Close(mgr)
	seq, err := mgr.WriteBuf(1, []byte("hello"))
	should.NoError(err)
	should.Equal(uint64(1), seq)
	buf, err := mgr.ReadBuf(1, 5)
	should.NoError(err)
	should.Equal("hello", string(buf))
}

func Test_lock_unlock(t *testing.T) {
	os.RemoveAll("/tmp/store")
	os.Mkdir("/tmp/store", 0777)
	mgr := New("/tmp/store", 4)
	defer plz.Close(mgr)
	mgr.Lock(0)
	mgr.Unlock(0)
}

func Test_remove_without_lock(t *testing.T) {
	should := require.New(t)
	os.RemoveAll("/tmp/store")
	os.Mkdir("/tmp/store", 0777)
	mgr := New("/tmp/store", 4)
	defer plz.Close(mgr)
	mgr.WriteBuf(1, []byte("hello"))
	_, err := os.Stat("/tmp/store/0")
	should.NoError(err)
	mgr.Remove(16)
	time.Sleep(time.Second)
	_, err = os.Stat("/tmp/store/0")
	should.Error(err)
}

func Test_remove_with_lock(t *testing.T) {
	should := require.New(t)
	os.RemoveAll("/tmp/store")
	os.Mkdir("/tmp/store", 0777)
	mgr := New("/tmp/store", 4)
	defer plz.Close(mgr)
	mgr.WriteBuf(1, []byte("hello"))
	_, err := os.Stat("/tmp/store/0")
	should.NoError(err)
	mgr.Lock(3)
	mgr.Remove(16)
	_, err = os.Stat("/tmp/store/0")
	should.NoError(err)
	mgr.Unlock(3)
	mgr.Remove(16)
	time.Sleep(time.Second)
	_, err = os.Stat("/tmp/store/0.dat")
	should.Error(err)
}

func Test_remove_large_file_block_seq(t *testing.T) {
	should := require.New(t)
	os.RemoveAll("/tmp/store")
	os.Mkdir("/tmp/store", 0777)
	mgr := New("/tmp/store", 4)
	defer plz.Close(mgr)
	mgr.WriteBuf(1024*1024*1024, []byte("hello"))
	_, err := os.Stat("/tmp/store/67108864")
	should.NoError(err)
	mgr.Remove(1024*1024*1024 + 1024)
	time.Sleep(time.Second)
	_, err = os.Stat("/tmp/store/67108864")
	should.Error(err)
}
