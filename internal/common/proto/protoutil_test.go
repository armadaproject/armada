package protoutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/testutil"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	msg              = &armadaevents.CancelJob{JobId: uuid.NewString()}
	compressor       = compress.NewThreadSafeZlibCompressor(1024)
	decompressor     = compress.NewThreadSafeZlibDecompressor()
	marshalledMsg, _ = proto.Marshal(msg)
	compressedMsg, _ = compressor.Compress(marshalledMsg)
	invalidMsg       = []byte{0x3}
)

func TestUnmarshall_Valid(t *testing.T) {
	unmarshalled, err := Unmarshall(marshalledMsg, &armadaevents.CancelJob{})
	require.NoError(t, err)
	testutil.AssertProtoEqual(t, msg, unmarshalled)
}

func TestUnmarshall_Invalid(t *testing.T) {
	_, err := Unmarshall(invalidMsg, &armadaevents.CancelJob{})
	require.Error(t, err)
}

func TestMustUnmarshall(t *testing.T) {
	unmarshalled := MustUnmarshall(marshalledMsg, &armadaevents.CancelJob{})
	testutil.AssertProtoEqual(t, msg, unmarshalled)
}

func TestDecompressAndUnmarshall_Valid(t *testing.T) {
	unmarshalled, err := DecompressAndUnmarshall(compressedMsg, &armadaevents.CancelJob{}, decompressor)
	require.NoError(t, err)
	testutil.AssertProtoEqual(t, msg, unmarshalled)
}

func TestDecompressAndUnmarshall_Invalid(t *testing.T) {
	_, err := DecompressAndUnmarshall(invalidMsg, &armadaevents.CancelJob{}, decompressor)
	require.Error(t, err)
}

func TestMustDecompressAndUnmarshall(t *testing.T) {
	unmarshalled := MustDecompressAndUnmarshall(compressedMsg, &armadaevents.CancelJob{}, decompressor)
	testutil.AssertProtoEqual(t, msg, unmarshalled)
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

func TestToTimestamp(t *testing.T) {
	tests := map[string]struct {
		ts *timestamppb.Timestamp
		t  time.Time
	}{
		"unix epoch": {
			ts: &timestamppb.Timestamp{Seconds: 0, Nanos: 0},
			t:  utcDate(1970, 1, 1),
		},
		"before unix epoch": {
			ts: &timestamppb.Timestamp{Seconds: -281836800, Nanos: 0},
			t:  utcDate(1961, 1, 26),
		},
		"after unix epoch": {
			ts: &timestamppb.Timestamp{Seconds: 1296000000, Nanos: 0},
			t:  utcDate(2011, 1, 26),
		},
		"after the epoch, in the middle of the day": {
			ts: &timestamppb.Timestamp{Seconds: 1296012345, Nanos: 940483},
			t:  time.Date(2011, 1, 26, 3, 25, 45, 940483, time.UTC),
		},
	}

	for name, tc := range tests {
		testName := fmt.Sprintf("ToTimestamp: %s", name)
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.ts, ToTimestamp(tc.t))
		})
	}

	for name, tc := range tests {
		testName := fmt.Sprintf("ToStdTime: %s", name)
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, tc.t, ToStdTime(tc.ts))
		})
	}
}

func TestToStdDuration(t *testing.T) {
	tests := map[string]struct {
		protoDuration *durationpb.Duration
		stdDuration   time.Duration
	}{
		"empty": {
			protoDuration: &durationpb.Duration{Seconds: 0, Nanos: 0},
			stdDuration:   0 * time.Second,
		},
		"seconds": {
			protoDuration: &durationpb.Duration{Seconds: 100, Nanos: 0},
			stdDuration:   100 * time.Second,
		},
		"seconds and nanos": {
			protoDuration: &durationpb.Duration{Seconds: 100, Nanos: 1000},
			stdDuration:   100*time.Second + 1000*time.Nanosecond,
		},
		"negative": {
			protoDuration: &durationpb.Duration{Seconds: -100, Nanos: -1000},
			stdDuration:   -100*time.Second - 1000*time.Nanosecond,
		},
		"nil": {
			protoDuration: nil,
			stdDuration:   0 * time.Second,
		},
	}
	timestamppb.Now()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.stdDuration, ToStdDuration(tc.protoDuration))
		})
	}
}

func TestToDuration(t *testing.T) {
	tests := map[string]struct {
		stdDuration   time.Duration
		protoDuration *durationpb.Duration
	}{
		"empty": {
			stdDuration:   0 * time.Second,
			protoDuration: &durationpb.Duration{Seconds: 0, Nanos: 0},
		},
		"seconds": {
			stdDuration:   100 * time.Second,
			protoDuration: &durationpb.Duration{Seconds: 100, Nanos: 0},
		},
		"seconds and nanos": {
			stdDuration:   100*time.Second + 1000*time.Nanosecond,
			protoDuration: &durationpb.Duration{Seconds: 100, Nanos: 1000},
		},
		"negative": {
			stdDuration:   -100*time.Second - 1000*time.Nanosecond,
			protoDuration: &durationpb.Duration{Seconds: -100, Nanos: -1000},
		},
	}
	timestamppb.Now()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.protoDuration, ToDuration(tc.stdDuration))
		})
	}
}

func utcDate(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}
