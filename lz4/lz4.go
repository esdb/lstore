package lz4

import (
	_ "github.com/cockroachdb/c-lz4"
	"unsafe"
)

// #cgo CPPFLAGS: -I ../../../cockroachdb/c-lz4/internal/lib
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #include "lz4.h"
import "C"

func VersionNumber() int {
	return int(C.LZ4_versionNumber())
}

func CompressBound(isize int) int {
	return (isize) + ((isize) / 255) + 16
}

func CompressDefault(input []byte, output []byte) int {
	ptrInput := (unsafe.Pointer)(&input)
	ptrOutput := (unsafe.Pointer)(&output)
	compressedSize := C.LZ4_compress_default(
		(*C.char)((*sliceHeader)(ptrInput).Data),
		(*C.char)((*sliceHeader)(ptrOutput).Data),
		C.int((*sliceHeader)(ptrInput).Len),
		C.int((*sliceHeader)(ptrOutput).Len))
	return int(compressedSize)
}

func DecompressSafe(input []byte, output []byte) int {
	ptrInput := (unsafe.Pointer)(&input)
	ptrOutput := (unsafe.Pointer)(&output)
	decompressedSize := C.LZ4_decompress_safe(
		(*C.char)((*sliceHeader)(ptrInput).Data),
		(*C.char)((*sliceHeader)(ptrOutput).Data),
		C.int((*sliceHeader)(ptrInput).Len),
		C.int((*sliceHeader)(ptrOutput).Len))
	return int(decompressedSize)
}

// sliceHeader is a safe version of SliceHeader used within this package.
type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}
