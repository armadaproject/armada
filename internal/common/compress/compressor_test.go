package compress

import (
	"bytes"
	"compress/zlib"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleWithCompression(t *testing.T) {
	compressor, err := NewZlibCompressor(0)
	assert.NoError(t, err)
	testSimple(t, compressor)
}

func TestSimpleWithNoCompression(t *testing.T) {
	compressor, err := NewZlibCompressor(1024 * 1024)
	assert.NoError(t, err)
	testSimple(t, compressor)
}

func TestEmptyWithCompression(t *testing.T) {
	compressor, err := NewZlibCompressor(0)
	assert.NoError(t, err)
	testEmpty(t, compressor)
}

func TestEmptyWithNoCompression(t *testing.T) {
	compressor, err := NewZlibCompressor(1024 * 1024)
	assert.NoError(t, err)
	testEmpty(t, compressor)
}

func testEmpty(t *testing.T, compressor Compressor) {
	input := ""
	compressed, err := compressor.Compress([]byte(input))
	assert.NoError(t, err)
	decompressed, err := decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, input, decompressed)
}

func testSimple(t *testing.T, compressor Compressor) {
	input := "hello world"
	compressed, err := compressor.Compress([]byte(input))
	assert.NoError(t, err)
	decompressed, err := decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, input, decompressed)

	// Check that if we compress again then all is ok
	// We do this check to ensure we've reset the buffers in the writer
	input2 := "The quick brown fox jumps over the lazy dog"
	compressed2, err := compressor.Compress([]byte(input2))
	assert.NoError(t, err)
	decompressed2, err := decompress(compressed2)
	assert.NoError(t, err)
	assert.Equal(t, input2, decompressed2)
}

func decompress(b []byte) (string, error) {
	compressedReader := bytes.NewReader(b)
	z, err := zlib.NewReader(compressedReader)
	if err != nil {
		return "", err
	}
	p, err := io.ReadAll(z)
	if err != nil {
		return "", err
	}
	return string(p), err
}
