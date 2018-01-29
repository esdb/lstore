package lz4

import (
	"github.com/pierrec/lz4"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func Test_VersionNumber(t *testing.T) {
	should := require.New(t)
	should.Equal(10701, VersionNumber())
}

func Test_CompressDefault(t *testing.T) {
	should := require.New(t)
	input := []byte{1, 2, 3}
	output := make([]byte, CompressBound(len(input)))
	compressedSize := CompressDefault(input, output)
	output = output[:compressedSize]
	input = make([]byte, len(input))
	should.Equal(len(input), DecompressSafe(output, input))
}

func Benchmark_cgo(b *testing.B) {
	input, err := ioutil.ReadFile("/tmp/orig-session.json")
	if err != nil {
		b.Error(err)
		return
	}
	output := make([]byte, CompressBound(len(input)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compressedSize := CompressDefault(input, output)
		compressed := output[:compressedSize]
		DecompressSafe(compressed, input)
	}
}

func Benchmark_pierrec_lz4(b *testing.B) {
	input, err := ioutil.ReadFile("/tmp/orig-session.json")
	if err != nil {
		b.Error(err)
		return
	}
	output := make([]byte, CompressBound(len(input)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compressedSize, _ := lz4.CompressBlock(input, output, 0)
		compressed := output[:compressedSize]
		lz4.UncompressBlock(compressed, input, 0)
	}
}
