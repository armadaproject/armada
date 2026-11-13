package conversions

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

const validDebugInfo = `{
  "schemaVersion": 1,
  "trigger": "cancelledNotStarted",
  "pod": {
    "phase": "Pending",
    "restartPolicy": "Never",
    "containers": [{"name": "broken", "state": "waiting", "reason": "ImagePullBackOff"}],
    "events": [
      {"type": "Normal", "reason": "Scheduled", "from": "default-scheduler", "message": "assigned", "timestamp": "2026-11-03T20:05:25Z"},
      {"type": "Warning", "reason": "Failed", "from": "kubelet", "message": "Error: ErrImagePull", "timestamp": "2026-11-03T20:06:06Z"}
    ]
  },
  "node": {
    "name": "node-1",
    "exists": true,
    "unschedulable": false,
    "ready": "True",
    "conditions": [{"type": "DiskPressure", "status": "False", "reason": "KubeletHasNoDiskPressure"}]
  }
}`

func TestParseDebugInfo_ValidPayload(t *testing.T) {
	info := ParseDebugInfo(validDebugInfo)

	require.NotNil(t, info)
	assert.Equal(t, int32(1), info.SchemaVersion)
	assert.Equal(t, "cancelledNotStarted", info.Trigger)
	assert.Equal(t, "Pending", info.Pod.Phase)
	assert.Equal(t, []*models.DebugContainer{{Name: "broken", State: "waiting", Reason: "ImagePullBackOff"}}, info.Pod.Containers)
	require.Len(t, info.Pod.Events, 2)
	assert.Equal(t, "Warning", info.Pod.Events[1].Type)
	assert.Equal(t, "Error: ErrImagePull", info.Pod.Events[1].Message)
	assert.Equal(t, "node-1", info.Node.Name)
	assert.Equal(t, pointer.Bool(true), info.Node.Exists)
	assert.Equal(t, pointer.Bool(false), info.Node.Unschedulable)
	assert.Equal(t, "DiskPressure", info.Node.Conditions[0].Type)
}

func TestParseDebugInfo_ToleratesFieldsFromANewerExecutor(t *testing.T) {
	info := ParseDebugInfo(`{"schemaVersion": 1, "trigger": "podFailed", "pod": {"phase": "Failed", "somethingNew": 7}, "extra": {}}`)

	require.NotNil(t, info)
	assert.Equal(t, "Failed", info.Pod.Phase)
}

func TestParseDebugInfo_ReturnsNilWhenTheMessageIsNotAUsablePayload(t *testing.T) {
	tests := map[string]string{
		"plain text":                   "Failed to pull image from registry: RateLimitExceeded",
		"empty":                        "",
		"json of another shape":        `{"diagnostic": {"status": "Failed"}}`,
		"json array":                   `[1, 2]`,
		"json null":                    `null`,
		"unsupported schema version":   `{"schemaVersion": 2, "trigger": "podFailed", "pod": {}}`,
		"missing schema version":       `{"trigger": "podFailed", "pod": {}}`,
		"missing pod":                  `{"schemaVersion": 1, "trigger": "podFailed"}`,
		"missing trigger":              `{"schemaVersion": 1, "pod": {}}`,
		"events is not a list":         `{"schemaVersion": 1, "trigger": "podFailed", "pod": {"events": "nope"}}`,
		"truncated json":               `{"schemaVersion": 1, "trigger": "pod`,
		"schema version of wrong type": `{"schemaVersion": "1", "trigger": "podFailed", "pod": {}}`,
	}

	for name, message := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Nil(t, ParseDebugInfo(message))
		})
	}
}

func TestParseDebugInfo_OmitsEmptyListsWhenSerialised(t *testing.T) {
	info := ParseDebugInfo(`{"schemaVersion": 1, "trigger": "podFailed", "pod": {"phase": "Failed"}}`)
	require.NotNil(t, info)

	serialised, err := json.Marshal(info)
	require.NoError(t, err)

	assert.JSONEq(t, `{"schemaVersion": 1, "trigger": "podFailed", "pod": {"phase": "Failed"}}`, string(serialised))
}

func TestParseDebugInfo_KeepsThePayloadWhenARecordInsideItIsIncomplete(t *testing.T) {
	// The executor copies pod and node records as Kubernetes reports them without checking them, so an
	// incomplete one must not cost the reader everything else that was captured.
	info := ParseDebugInfo(`{
	  "schemaVersion": 1,
	  "trigger": "podFailed",
	  "pod": {
	    "phase": "Failed",
	    "containers": [{"name": "main"}],
	    "events": [{"type": "Warning", "reason": "Failed"}, null]
	  },
	  "node": {
	    "conditions": [
	      {"type": "NetworkUnavailable", "status": ""},
	      {"type": "Ready", "status": "True"}
	    ]
	  }
	}`)

	require.NotNil(t, info)
	assert.Equal(t, "Failed", info.Pod.Phase)
	assert.Equal(t, []*models.DebugContainer{{Name: "main"}}, info.Pod.Containers)
	assert.Len(t, info.Pod.Events, 1, "a null entry in a list is dropped rather than sent on to the UI")
	require.NotNil(t, info.Node)
	assert.Equal(t, []*models.DebugCondition{
		{Type: "NetworkUnavailable"},
		{Type: "Ready", Status: "True"},
	}, info.Node.Conditions)
}
