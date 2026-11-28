package schedulerobjects

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	types "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every field of ExecutorSummary is set here, so a field added to the proto but missing from the JSON
// conversion makes the round trip fail.
func TestExecutorSummary_JSONRoundTrip(t *testing.T) {
	heartbeat := time.Date(2026, 11, 24, 10, 0, 0, 500, time.UTC)
	in := &ExecutorSummary{
		Name:           "executor-a",
		Pool:           "cpu",
		LastUpdateTime: &types.Timestamp{Seconds: heartbeat.Unix(), Nanos: int32(heartbeat.Nanosecond())},
		NodeCount:      3,
		Cordoned:       true,
		CordonReason:   "maintenance",
		CordonedBy:     "alice",
	}
	v := reflect.ValueOf(*in)
	for i := 0; i < v.NumField(); i++ {
		require.Falsef(t, v.Field(i).IsZero(), "field %s is not set in the test, add it", v.Type().Field(i).Name)
	}

	b, err := json.Marshal(in)
	require.NoError(t, err)
	out := &ExecutorSummary{}
	require.NoError(t, json.Unmarshal(b, out))
	assert.Equal(t, in, out)
}

func TestExecutorSummary_JSONKeepsZeroValues(t *testing.T) {
	b, err := json.Marshal(&ExecutorSummary{Name: "executor-a"})
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(b, &doc))
	assert.Equal(t, false, doc["cordoned"])
	assert.EqualValues(t, 0, doc["nodeCount"])
	assert.NotContains(t, doc, "lastUpdateTime")
}
