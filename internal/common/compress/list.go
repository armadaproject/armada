package compress

import (
	"bytes"
	"encoding/gob"
)

func CompressStringArray(input []string, compressor Compressor) ([]byte, error) {
	if len(input) <= 0 {
		return []byte{}, nil
	}

	buf := &bytes.Buffer{}
	err := gob.NewEncoder(buf).Encode(input)
	if err != nil {
		return nil, err
	}
	bs := buf.Bytes()

	return compressor.Compress(bs)
}

func MustCompressStringArray(input []string, compressor Compressor) []byte {
	b, err := CompressStringArray(input, compressor)
	if err != nil {
		panic(err)
	}
	return b
}

func DecompressStringArray(input []byte, decompressor Decompressor) ([]string, error) {
	if len(input) <= 0 {
		return []string{}, nil
	}

	decompressedValue, err := decompressor.Decompress(input)
	if err != nil {
		return nil, err
	}

	var data []string
	buf := bytes.NewBuffer(decompressedValue)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(&data)
	return data, err
}

func MustDecompressStringArray(input []byte, compressor Decompressor) []string {
	data, err := DecompressStringArray(input, compressor)
	if err != nil {
		panic(err)
	}
	return data
}
