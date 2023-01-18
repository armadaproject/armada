package compress

import (
	"bytes"
	"compress/zlib"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleDecompressWithCompression(t *testing.T) {
	decompressor := NewZlibDecompressor()
	testSimpleDecompress(t, decompressor)
}

func testSimpleDecompress(t *testing.T, decompressor Decompressor) {
	input := "hello world"
	compressed, err := compress(input)
	assert.NoError(t, err)
	decompressed, err := decompressor.Decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, input, string(decompressed))

	// Check that if we decompress again then all is ok
	// We do this check to ensure we've reset the buffers in the reader
	input2 := "The quick brown fox jumps over the lazy dog"
	compressed2, err := compress(input2)
	assert.NoError(t, err)
	decompressed2, err := decompressor.Decompress(compressed2)
	assert.NoError(t, err)
	assert.Equal(t, input2, string(decompressed2))
}

func compress(s string) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write([]byte(s))
	w.Close()
	return b.Bytes(), err
}
