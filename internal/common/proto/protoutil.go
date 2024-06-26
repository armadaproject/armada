package protoutil

import (
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/compress"
)

// Unmarshall unmarshalls a proto message in a type-safe way.
// Hopefully go-generics will one day be powerful enough that we won't need the msg param
func Unmarshall[T proto.Message](buf []byte, msg T) (T, error) {
	err := proto.Unmarshal(buf, msg)
	return msg, err
}

// DecompressAndUnmarshall first decompressed the message and then unmarshalls
func DecompressAndUnmarshall[T proto.Message](buf []byte, msg T, decompressor compress.Decompressor) (T, error) {
	decompressed, err := decompressor.Decompress(buf)
	if err != nil {
		return msg, err
	}
	return Unmarshall(decompressed, msg)
}

// MustUnmarshall unmarshalls a proto message and panics if the unmarshall fails.  The main use case here is for unit tests.
// Think carefully if you intend to use this elsewhere
func MustUnmarshall[T proto.Message](buf []byte, msg T) T {
	msg, err := Unmarshall(buf, msg)
	if err != nil {
		panic(errors.Wrap(err, "Error unmarshalling object"))
	}
	return msg
}

// MustDecompressAndUnmarshall first decompressed the message and then unmarshalls.  If either of these steps fail then it will panic.
// The main use case here is for unit tests. Think carefully if you intend to use this elsewhere
func MustDecompressAndUnmarshall[T proto.Message](buf []byte, msg T, decompressor compress.Decompressor) T {
	msg, err := DecompressAndUnmarshall(buf, msg, decompressor)
	if err != nil {
		panic(errors.Wrap(err, "Error unmarshalling object"))
	}
	return msg
}

// MarshallAndCompress first marshalls the supplied proto message and then compresses it.
func MarshallAndCompress(msg proto.Message, compressor compress.Compressor) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, errors.Wrap(err, "Error marshalling object")
	}
	return compressor.Compress(b)
}

// MustMarshall marshalls a proto message and panics if the unmarshall fails.  The main use case here is for unit tests.
// Think carefully if you intend to use this elsewhere
func MustMarshall(msg proto.Message) []byte {
	b, err := proto.Marshal(msg)
	if err != nil {
		panic(errors.Wrap(err, "Error marshalling object"))
	}
	return b
}

// MustMarshallAndCompress first marshalls the supplied proto message and then compresses it.
// If either of these steps fail then it will panic. The main use case here is for unit tests.
// Think carefully if you intend to use this elsewhere
func MustMarshallAndCompress(msg proto.Message, compressor compress.Compressor) []byte {
	b, err := MarshallAndCompress(msg, compressor)
	if err != nil {
		panic(err)
	}
	return b
}

func ToStdTime(ts *types.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}.UTC()
	}

	return time.Unix(ts.Seconds, int64(ts.Nanos)).UTC()
}

func ToTimestamp(t time.Time) *types.Timestamp {
	return &types.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}
