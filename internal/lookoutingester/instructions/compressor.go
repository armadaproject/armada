package instructions

import (
	"bytes"
	"compress/zlib"
)

// Compressor is a fast, single threaded compressor.
// This type allows us to reuse buffers etc for performance
type Compressor interface {
	// Compress compresses the byte array
	Compress(b []byte) ([]byte, error)
}

// NoOpCompressor is a Compressor that does nothing.  Useful for tests.
type NoOpCompressor struct {
}

func (c *NoOpCompressor) Compress(b []byte) ([]byte, error) {
	return b, nil
}

// ZlibCompressor compresses to Zlib, which for KB size payloads seems more (cpu) efficient than the newer formats
// such as zstd. The compressor will only compress if the msg is greater than minCompressSize
type ZlibCompressor struct {
	buffer             *bytes.Buffer
	compressedWriter   *zlib.Writer
	unCompressedWriter *zlib.Writer
	minCompressSize    int
}

func NewZlibCompressor(minCompressSize int) (*ZlibCompressor, error) {

	var b bytes.Buffer
	compressedWriter, err := zlib.NewWriterLevel(&b, zlib.BestSpeed)
	if err != nil {
		return nil, err
	}
	unCompressedWriter, err := zlib.NewWriterLevel(&b, zlib.NoCompression)
	if err != nil {
		return nil, err
	}

	return &ZlibCompressor{
		buffer:             &b,
		compressedWriter:   compressedWriter,
		unCompressedWriter: unCompressedWriter,
		minCompressSize:    minCompressSize,
	}, nil
}

func (c *ZlibCompressor) Compress(b []byte) ([]byte, error) {

	var writer = c.unCompressedWriter
	if len(b) > c.minCompressSize {
		writer = c.compressedWriter
	}
	c.buffer.Reset()
	writer.Reset(c.buffer)

	_, err := writer.Write(b)
	if err != nil {
		return nil, err
	}

	// For some reason writer.Flush() doesn't work here
	err = writer.Close()
	if err != nil {
		return nil, err
	}
	compressed := make([]byte, len(c.buffer.Bytes()))
	copy(compressed, c.buffer.Bytes())
	return compressed, nil
}
