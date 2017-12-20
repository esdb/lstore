package lstore

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_store(t *testing.T) {
	should := require.New(t)
	store := NewStore("/tmp/lstore.bin")
	offset, err := store.Write(0, Row{"hello"})
	should.Equal(Offset(0), offset)
	should.Nil(err)
}