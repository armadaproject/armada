package replay

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/xitongsys/parquet-go/parquet"
)

// The workload profile has flat, non-repeated signed integers and UTF-8
// strings. Decode only supported page encodings, bounding all allocations by
// verified page sizes and row counts rather than by lengths inside the data.
func (c *columnPages) decode(header *parquet.PageHeader, payload []byte, values int) ([]any, error) {
	codec := c.meta.Codec
	var definitions []byte
	unpacked := int(header.UncompressedPageSize)
	var encoding parquet.Encoding
	switch header.Type {
	case parquet.PageType_DATA_PAGE_V2:
		v2 := header.DataPageHeaderV2
		length := int(v2.DefinitionLevelsByteLength)
		definitions, payload = payload[:length], payload[length:]
		unpacked -= length
		if !v2.GetIsCompressed() {
			codec = parquet.CompressionCodec_UNCOMPRESSED
		}
		encoding = v2.Encoding
	case parquet.PageType_DATA_PAGE:
		encoding = header.DataPageHeader.Encoding
	case parquet.PageType_DICTIONARY_PAGE:
		encoding = header.DictionaryPageHeader.Encoding
	}
	raw, err := unpackPage(payload, unpacked, codec)
	if err != nil {
		return nil, err
	}
	if header.Type == parquet.PageType_DICTIONARY_PAGE {
		if encoding != parquet.Encoding_PLAIN && encoding != parquet.Encoding_PLAIN_DICTIONARY {
			return nil, fmt.Errorf("unsupported dictionary-page encoding %s", encoding)
		}
		return c.decodePlain(raw, values)
	}
	if c.optional && header.Type == parquet.PageType_DATA_PAGE {
		if header.DataPageHeader.DefinitionLevelEncoding != parquet.Encoding_RLE || len(raw) < 4 {
			return nil, fmt.Errorf("expected length-prefixed RLE definition levels")
		}
		length := binary.LittleEndian.Uint32(raw[:4])
		raw = raw[4:]
		if uint64(length) > uint64(len(raw)) {
			return nil, io.ErrUnexpectedEOF
		}
		definitions, raw = raw[:length], raw[length:]
	}
	present := values
	var levels []uint32
	if c.optional {
		levels, err = readHybrid(definitions, 1, values)
		if err != nil {
			return nil, fmt.Errorf("definition levels: %w", err)
		}
		for _, level := range levels {
			present -= int(1 - level)
		}
	}
	if header.Type == parquet.PageType_DATA_PAGE_V2 && present != values-int(header.DataPageHeaderV2.NumNulls) {
		return nil, fmt.Errorf("definition levels disagree with data-page null count")
	}
	var data []any
	switch encoding {
	case parquet.Encoding_PLAIN:
		data, err = c.decodePlain(raw, present)
	case parquet.Encoding_RLE_DICTIONARY, parquet.Encoding_PLAIN_DICTIONARY:
		data, err = c.decodeDictionary(raw, present)
	default:
		return nil, fmt.Errorf("unsupported data-page encoding %s", encoding)
	}
	if err != nil {
		return nil, err
	}
	if present == values {
		return data, nil
	}
	expanded := make([]any, values)
	j := 0
	for i, level := range levels {
		if level != 0 {
			expanded[i] = data[j]
			j++
		}
	}
	return expanded, nil
}

func unpackPage(payload []byte, unpacked int, codec parquet.CompressionCodec) ([]byte, error) {
	if unpacked < 0 || unpacked > maxPageBytes {
		return nil, fmt.Errorf("invalid uncompressed size %d", unpacked)
	}
	if codec == parquet.CompressionCodec_UNCOMPRESSED {
		if len(payload) != unpacked {
			return nil, fmt.Errorf("uncompressed page length disagrees with header")
		}
		return payload, nil
	}
	decoded, err := snappy.DecodedLen(payload)
	if err != nil || decoded != unpacked {
		return nil, fmt.Errorf("Snappy length disagrees with Parquet page")
	}
	return snappy.Decode(nil, payload)
}

func (c *columnPages) decodePlain(raw []byte, count int) ([]any, error) {
	values := make([]any, count)
	for i := range values {
		switch c.meta.Type {
		case parquet.Type_INT64:
			if len(raw) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			values[i] = int64(binary.LittleEndian.Uint64(raw[:8]))
			raw = raw[8:]
		case parquet.Type_BYTE_ARRAY:
			if len(raw) < 4 {
				return nil, io.ErrUnexpectedEOF
			}
			length := binary.LittleEndian.Uint32(raw[:4])
			raw = raw[4:]
			if uint64(length) > uint64(len(raw)) || length > maxLogicalRowBytes {
				return nil, fmt.Errorf("string length %d exceeds page or logical row bounds", length)
			}
			values[i] = string(raw[:length])
			raw = raw[length:]
		}
	}
	if len(raw) != 0 {
		return nil, fmt.Errorf("page contains bytes beyond its declared values")
	}
	return values, nil
}

func (c *columnPages) decodeDictionary(raw []byte, count int) ([]any, error) {
	if count == 0 && len(raw) == 0 {
		return nil, nil
	}
	if len(raw) == 0 || raw[0] > 32 {
		return nil, fmt.Errorf("missing or invalid dictionary bit width")
	}
	indices, err := readHybrid(raw[1:], int(raw[0]), count)
	if err != nil {
		return nil, err
	}
	values := make([]any, count)
	for i, index := range indices {
		if uint64(index) >= uint64(len(c.dictionary)) {
			return nil, fmt.Errorf("dictionary index %d is outside the dictionary", index)
		}
		values[i] = c.dictionary[index]
	}
	return values, nil
}

// The page value count bounds RLE runs and packed groups. A final packed group
// may be padded by the writer. Padding never appears in the returned values.
func readHybrid(raw []byte, width, count int) ([]uint32, error) {
	values := make([]uint32, 0, count)
	for len(raw) > 0 && len(values) < count {
		header, length := binary.Uvarint(raw)
		if length <= 0 || header < 2 {
			return nil, fmt.Errorf("invalid RLE/bit-packed run header")
		}
		raw = raw[length:]
		run := header >> 1
		remaining := count - len(values)
		if header&1 == 0 {
			bytes := (width + 7) / 8
			if run > uint64(remaining) || bytes > len(raw) {
				return nil, fmt.Errorf("RLE run exceeds the page value count or bytes")
			}
			var value uint32
			for i := range bytes {
				value |= uint32(raw[i]) << (8 * i)
			}
			if value>>width != 0 {
				return nil, fmt.Errorf("RLE value exceeds declared bit width")
			}
			for range int(run) {
				values = append(values, value)
			}
			raw = raw[bytes:]
			continue
		}
		if width > 0 && run > uint64(len(raw)/width) {
			return nil, fmt.Errorf("bit-packed run exceeds the page bytes")
		}
		bytes := int(run * uint64(width))
		// Some writers pad a final packed run beyond the next group of
		// eight. The encoded byte count is bounded above and padding
		// does not increase the number of values decoded.
		filled := remaining
		if run < uint64((remaining+7)/8) {
			filled = int(run) * 8
		}
		for i := range filled {
			var value uint32
			for bit := range width {
				offset := i*width + bit
				value |= uint32((raw[offset/8]>>uint(offset%8))&1) << bit
			}
			values = append(values, value)
		}
		raw = raw[bytes:]
	}
	if len(values) != count || len(raw) != 0 {
		return nil, fmt.Errorf("RLE/bit-packed runs disagree with page value count")
	}
	return values, nil
}
