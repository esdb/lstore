package mheap

import (
	"testing"
	"github.com/stretchr/testify/require"
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
}