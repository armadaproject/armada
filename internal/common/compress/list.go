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
