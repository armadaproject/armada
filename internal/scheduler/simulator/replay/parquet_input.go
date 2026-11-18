package replay

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/xitongsys/parquet-go/parquet"
)

const (
	maxFooterBytes = 8 << 20
	maxGroups      = 1024
	maxGroupRows   = 65536
	maxGroupDisk   = 32 << 20
	maxGroupMemory = 64 << 20
	maxPageBytes   = 8 << 20
	maxPageValues  = 1024
	maxPageHeader  = 64 << 10
)

type workloadColumn struct {
	name     string
	kind     parquet.Type
	required bool
}

var workloadColumns = []workloadColumn{
	{"logical_job_id", parquet.Type_BYTE_ARRAY, true},
	{"submitted_at_us", parquet.Type_INT64, true},
	{"queue", parquet.Type_BYTE_ARRAY, true},
	{"job_set", parquet.Type_BYTE_ARRAY, true},
	{"queue_priority", parquet.Type_INT64, true},
	{"priority_class", parquet.Type_BYTE_ARRAY, true},
	{"useful_work_us", parquet.Type_INT64, true},
	{"startup_us", parquet.Type_INT64, false},
	{"checkpoint_interval_us", parquet.Type_INT64, false},
	{"requirements_json", parquet.Type_BYTE_ARRAY, true},
	{"gang_id", parquet.Type_BYTE_ARRAY, false},
	{"gang_cardinality", parquet.Type_INT64, false},
	{"gang_node_uniformity_label", parquet.Type_BYTE_ARRAY, false},
	{"work_estimation_method", parquet.Type_BYTE_ARRAY, false},
}

type partitionReader struct {
	file     *os.File
	footer   *parquet.FileMetaData
	optional map[string]bool
	group    int
	row      int64
	pages    []columnPages
}

func readParquetPartition(ctx context.Context, file *os.File) (*partitionReader, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if !info.Mode().IsRegular() || size < 12 {
		return nil, fmt.Errorf("expected a regular Parquet file with a footer")
	}
	var first [4]byte
	var last [8]byte
	if _, err := file.ReadAt(first[:], 0); err != nil {
		return nil, err
	}
	if _, err := file.ReadAt(last[:], size-8); err != nil {
		return nil, err
	}
	footerBytes := int64(binary.LittleEndian.Uint32(last[:4]))
	if string(first[:]) != "PAR1" || string(last[4:]) != "PAR1" ||
		footerBytes < 1 || footerBytes > maxFooterBytes || footerBytes > size-12 {
		return nil, fmt.Errorf("invalid Parquet magic or footer size %d", footerBytes)
	}
	footerStart := size - 8 - footerBytes
	section := io.NewSectionReader(file, footerStart, footerBytes)
	stream := thrift.NewStreamTransportR(section)
	footer := parquet.NewFileMetaData()
	protocol := thrift.NewTCompactProtocolConf(stream, &thrift.TConfiguration{MaxMessageSize: int32(footerBytes)})
	if err := footer.Read(ctx, boundedParquetLists{protocol}); err != nil {
		return nil, fmt.Errorf("decode Parquet footer: %w", err)
	}
	if len(footer.RowGroups) > maxGroups || footer.NumRows < 0 {
		return nil, fmt.Errorf("invalid partition row count or more than %d row groups", maxGroups)
	}
	if err := validateParquetSchema(footer.Schema); err != nil {
		return nil, err
	}
	columnTypes := make(map[string]parquet.Type, len(footer.Schema)-1)
	optional := make(map[string]bool, len(footer.Schema)-1)
	for _, leaf := range footer.Schema[1:] {
		columnTypes[leaf.Name] = *leaf.Type
		optional[leaf.Name] = leaf.GetRepetitionType() == parquet.FieldRepetitionType_OPTIONAL
	}
	var rows int64
	type fileRange struct{ start, end int64 }
	var chunks []fileRange
	for index, group := range footer.RowGroups {
		if group.NumRows < 0 || group.NumRows > maxGroupRows || len(group.Columns) != len(columnTypes) {
			return nil, fmt.Errorf("row group %d has invalid row or column count", index+1)
		}
		rows += group.NumRows
		var disk, memory int64
		seen := make(map[string]bool, len(group.Columns))
		for _, chunk := range group.Columns {
			m := chunk.MetaData
			if chunk.FilePath != nil || m == nil || len(m.PathInSchema) != 1 {
				return nil, fmt.Errorf("row group %d has external or missing column metadata", index+1)
			}
			name := m.PathInSchema[0]
			kind, ok := columnTypes[name]
			if !ok || seen[name] || m.Type != kind || m.NumValues != group.NumRows {
				return nil, fmt.Errorf("row group %d has unexpected metadata for column %q", index+1, name)
			}
			seen[name] = true
			start := chunkStart(m)
			if start < 4 || start > footerStart || m.DataPageOffset < start ||
				m.TotalCompressedSize < 0 || m.TotalCompressedSize > min(maxGroupDisk-disk, footerStart-start) ||
				m.TotalUncompressedSize < 0 || m.TotalUncompressedSize > maxGroupMemory-memory ||
				(group.NumRows > 0 && (m.TotalCompressedSize == 0 || m.DataPageOffset >= start+m.TotalCompressedSize)) {
				return nil, fmt.Errorf("row group %d column %q has invalid bounds", index+1, name)
			}
			switch m.Codec {
			case parquet.CompressionCodec_UNCOMPRESSED, parquet.CompressionCodec_SNAPPY:
			default:
				return nil, fmt.Errorf("row group %d column %q uses unsupported codec %s", index+1, name, m.Codec)
			}
			disk += m.TotalCompressedSize
			memory += m.TotalUncompressedSize
			if m.TotalCompressedSize != 0 {
				chunks = append(chunks, fileRange{start, start + m.TotalCompressedSize})
			}
		}
	}
	slices.SortFunc(chunks, func(a, b fileRange) int {
		if a.start < b.start {
			return -1
		}
		if a.start > b.start {
			return 1
		}
		return 0
	})
	for i := 1; i < len(chunks); i++ {
		if chunks[i].start < chunks[i-1].end {
			return nil, fmt.Errorf("Parquet column chunks overlap")
		}
	}
	if rows != footer.NumRows {
		return nil, fmt.Errorf("Parquet footer declares %d rows but row groups contain %d", footer.NumRows, rows)
	}
	return &partitionReader{file: file, footer: footer, optional: optional}, nil
}

// Parquet metadata uses Thrift lists whose declared lengths otherwise drive
// allocations before the generated schema reader has seen a single element.
type boundedParquetLists struct{ thrift.TProtocol }

func (p boundedParquetLists) ReadListBegin(ctx context.Context) (thrift.TType, int, error) {
	kind, count, err := p.TProtocol.ReadListBegin(ctx)
	if err == nil && (count < 0 || count > maxGroups) {
		err = fmt.Errorf("Parquet metadata list exceeds %d entries", maxGroups)
	}
	return kind, count, err
}

func validateParquetSchema(elements []*parquet.SchemaElement) error {
	if len(elements) == 0 || len(elements) > 1+len(workloadColumns) ||
		elements[0].Type != nil || elements[0].GetNumChildren() != int32(len(elements)-1) {
		return fmt.Errorf("expected a flat recorded workload schema")
	}
	names := make(map[string]*parquet.SchemaElement, len(elements)-1)
	for _, leaf := range elements[1:] {
		if leaf.Type == nil || leaf.GetNumChildren() != 0 || leaf.RepetitionType == nil ||
			(leaf.GetRepetitionType() != parquet.FieldRepetitionType_REQUIRED && leaf.GetRepetitionType() != parquet.FieldRepetitionType_OPTIONAL) ||
			names[leaf.Name] != nil {
			return fmt.Errorf("invalid Parquet leaf column %q", leaf.Name)
		}
		names[leaf.Name] = leaf
	}
	for _, column := range workloadColumns {
		leaf, ok := names[column.name]
		if !ok && !column.required {
			continue
		}
		if !ok || leaf.GetType() != column.kind {
			return fmt.Errorf("column %q must have physical type %s", column.name, column.kind)
		}
		if column.kind == parquet.Type_BYTE_ARRAY && !(leaf.IsSetConvertedType() && leaf.GetConvertedType() == parquet.ConvertedType_UTF8 ||
			leaf.LogicalType != nil && leaf.LogicalType.STRING != nil) {
			return fmt.Errorf("column %q must be UTF-8", column.name)
		}
		if column.kind == parquet.Type_INT64 {
			if leaf.IsSetConvertedType() && leaf.GetConvertedType() != parquet.ConvertedType_INT_64 ||
				(leaf.LogicalType != nil && (leaf.LogicalType.INTEGER == nil ||
					!leaf.LogicalType.INTEGER.IsSigned || leaf.LogicalType.INTEGER.BitWidth != 64)) {
				return fmt.Errorf("column %q must use plain or signed INT64", column.name)
			}
		}
		delete(names, column.name)
	}
	for name := range names {
		return fmt.Errorf("unknown workload column %q", name)
	}
	return nil
}

func chunkStart(m *parquet.ColumnMetaData) int64 {
	if m.DictionaryPageOffset != nil {
		return *m.DictionaryPageOffset
	}
	return m.DataPageOffset
}

type columnPages struct {
	meta       *parquet.ColumnMetaData
	offset     int64
	end        int64
	unpacked   int64
	values     []any
	dictionary []any
	optional   bool
}

func (p *partitionReader) next(ctx context.Context) (map[string]any, error) {
	for p.group < len(p.footer.RowGroups) {
		group := p.footer.RowGroups[p.group]
		if p.row >= group.NumRows {
			p.group++
			p.row = 0
			p.pages = nil
			continue
		}
		if p.pages == nil {
			p.pages = make([]columnPages, len(group.Columns))
			for i, column := range group.Columns {
				m := column.MetaData
				start := chunkStart(m)
				p.pages[i] = columnPages{meta: m, offset: start, end: start + m.TotalCompressedSize, optional: p.optional[m.PathInSchema[0]]}
			}
		}
		row := make(map[string]any, len(p.pages))
		for i := range p.pages {
			column := &p.pages[i]
			name := column.meta.PathInSchema[0]
			for len(column.values) == 0 {
				if err := column.load(ctx, p.file, group.NumRows-p.row); err != nil {
					return nil, fmt.Errorf("row group %d row %d column %q: %w", p.group+1, p.row+1, name, err)
				}
			}
			row[name] = column.values[0]
			column.values = column.values[1:]
			if p.row+1 == group.NumRows && (len(column.values) != 0 || column.offset != column.end ||
				column.unpacked != column.meta.TotalUncompressedSize) {
				return nil, fmt.Errorf("row group %d column %q has unaccounted page bytes or values", p.group+1, name)
			}
		}
		p.row++
		return row, nil
	}
	return nil, io.EOF
}

func (c *columnPages) load(ctx context.Context, file *os.File, remaining int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.offset == c.end {
		return io.ErrUnexpectedEOF
	}
	length := min(c.end-c.offset, maxPageHeader)
	section := io.NewSectionReader(file, c.offset, length)
	buffered := bufio.NewReader(section)
	stream := thrift.NewStreamTransportR(buffered)
	header := parquet.NewPageHeader()
	protocol := thrift.NewTCompactProtocolConf(stream, &thrift.TConfiguration{MaxMessageSize: maxPageHeader})
	if err := header.Read(ctx, boundedParquetLists{protocol}); err != nil {
		return fmt.Errorf("read page header: %w", err)
	}
	read, err := section.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	headerSize := read - int64(buffered.Buffered())
	compressed, uncompressed := int64(header.CompressedPageSize), int64(header.UncompressedPageSize)
	if compressed < 0 || compressed > min(maxPageBytes, c.end-c.offset-headerSize) ||
		uncompressed < 0 || uncompressed > maxPageBytes ||
		headerSize+uncompressed > c.meta.TotalUncompressedSize-c.unpacked {
		return fmt.Errorf("invalid page size (compressed %d, uncompressed %d)", compressed, uncompressed)
	}
	var values int64
	switch header.Type {
	case parquet.PageType_DICTIONARY_PAGE:
		if header.DictionaryPageHeader == nil || c.dictionary != nil || c.offset >= c.meta.DataPageOffset {
			return fmt.Errorf("unexpected dictionary page")
		}
		values = int64(header.DictionaryPageHeader.NumValues)
		remaining = maxPageValues
	case parquet.PageType_DATA_PAGE:
		if header.DataPageHeader == nil || c.offset < c.meta.DataPageOffset {
			return fmt.Errorf("unexpected data page")
		}
		values = int64(header.DataPageHeader.NumValues)
	case parquet.PageType_DATA_PAGE_V2:
		h := header.DataPageHeaderV2
		if h == nil || c.offset < c.meta.DataPageOffset || h.RepetitionLevelsByteLength != 0 ||
			h.DefinitionLevelsByteLength < 0 || int64(h.DefinitionLevelsByteLength) > compressed ||
			h.NumRows != h.NumValues || h.NumNulls < 0 || h.NumNulls > h.NumValues ||
			(!c.optional && h.DefinitionLevelsByteLength != 0) {
			return fmt.Errorf("unsupported or malformed data page v2")
		}
		values = int64(h.NumValues)
	default:
		return fmt.Errorf("unsupported page type %s", header.Type)
	}
	if values <= 0 || values > min(remaining, maxPageValues) {
		return fmt.Errorf("invalid page value count %d", values)
	}
	pageSize := headerSize + compressed
	payload := make([]byte, compressed)
	if _, err := file.ReadAt(payload, c.offset+headerSize); err != nil {
		return err
	}
	if header.Crc != nil && crc32.ChecksumIEEE(payload) != uint32(*header.Crc) {
		return fmt.Errorf("page CRC mismatch")
	}
	decoded, err := c.decode(header, payload, int(values))
	if err != nil {
		return err
	}

	c.offset += pageSize
	c.unpacked += headerSize + uncompressed
	if header.Type == parquet.PageType_DICTIONARY_PAGE {
		c.dictionary = decoded
	} else {
		c.values = decoded
	}
	return nil
}
