package protoutil

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	msg              = &armadaevents.CancelJob{JobId: armadaevents.ProtoUuidFromUuid(uuid.New())}
	compressor       = compress.NewThreadSafeZlibCompressor(1024)
	decompressor     = compress.NewThreadSafeZlibDecompressor()
	marshalledMsg, _ = proto.Marshal(msg)
	compressedMsg, _ = compressor.Compress(marshalledMsg)
	invalidMsg       = []byte{0x3}
)

func TestUnmarshall_Valid(t *testing.T) {
	unmarshalled, err := Unmarshall(marshalledMsg, &armadaevents.CancelJob{})
	require.NoError(t, err)
	assert.Equal(t, msg, unmarshalled)
}

func TestUnmarshall_Invalid(t *testing.T) {
	_, err := Unmarshall(invalidMsg, &armadaevents.CancelJob{})
	require.Error(t, err)
}

func TestMustUnmarshall(t *testing.T) {
	unmarshalled := MustUnmarshall(marshalledMsg, &armadaevents.CancelJob{})
	assert.Equal(t, msg, unmarshalled)
}

func TestDecompressAndUnmarshall_Valid(t *testing.T) {
	unmarshalled, err := DecompressAndUnmarshall(compressedMsg, &armadaevents.CancelJob{}, decompressor)
	require.NoError(t, err)
	assert.Equal(t, msg, unmarshalled)
}

func TestDecompressAndUnmarshall_Invalid(t *testing.T) {
	_, err := DecompressAndUnmarshall(invalidMsg, &armadaevents.CancelJob{}, decompressor)
	require.Error(t, err)
}

func TestMustDecompressAndUnmarshall(t *testing.T) {
	unmarshalled := MustDecompressAndUnmarshall(compressedMsg, &armadaevents.CancelJob{}, decompressor)
	assert.Equal(t, msg, unmarshalled)
}

func TestMarshallAndCompress(t *testing.T) {
	bytes, err := MarshallAndCompress(msg, compressor)
	require.NoError(t, err)
	assert.Equal(t, compressedMsg, bytes)
}

func TestMustMarshallAndCompress(t *testing.T) {
	bytes := MustMarshallAndCompress(msg, compressor)
	assert.Equal(t, compressedMsg, bytes)
}

func TestHash(t *testing.T) {
}
