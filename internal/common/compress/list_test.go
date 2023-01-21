package compress

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressAndDecompressGiveOriginalValue(t *testing.T) {
	compressor, err := NewZlibCompressor(0)
	assert.NoError(t, err)
	decompressor := NewZlibDecompressor()

	input := []string{"test", "array", "values"}

	compressedInput, err := CompressStringArray(input, compressor)
	assert.NoError(t, err)
	decompressedOutput, err := DecompressStringArray(compressedInput, decompressor)
	assert.NoError(t, err)
	assert.Equal(t, input, decompressedOutput)
}
