package conversions

import (
	"encoding/json"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
)

// supportedDebugSchemaVersion is the PodDebugInfo schema version the generated models describe. The executor
// increments its debugSchemaVersion (internal/executor/reporter/debug.go) when a field changes meaning or is
// removed, so a payload of another version is not safe to present as these models.
const supportedDebugSchemaVersion = 1

// ParseDebugInfo reads the debug message stored for a job run as the structured payload the executor captured.
//
// It returns nil when the message is not such a payload - text from before debug data was JSON, JSON of another
// shape, or a schema version this build does not know - so callers can fall back to presenting the message as it
// was stored. Fields this build does not know about are ignored.
//
// Only what the reader cannot do without is required: the trigger and the pod. The executor copies pod and node
// records as Kubernetes reports them, without checking them, so one incomplete record - say a node condition with
// no status - must not cost the reader everything else that was captured. Such a record is kept as it is.
func ParseDebugInfo(message string) *models.PodDebugInfo {
	info := &models.PodDebugInfo{}
	if err := json.Unmarshal([]byte(message), info); err != nil {
		return nil
	}
	if info.SchemaVersion != supportedDebugSchemaVersion || info.Trigger == "" || info.Pod == nil {
		return nil
	}

	info.Pod.InitContainers = dropNil(info.Pod.InitContainers)
	info.Pod.Containers = dropNil(info.Pod.Containers)
	info.Pod.Events = dropNil(info.Pod.Events)
	if info.Node != nil {
		info.Node.Conditions = dropNil(info.Node.Conditions)
		info.Node.Events = dropNil(info.Node.Events)
	}
	return info
}

// dropNil removes null entries, which JSON allows in a list but which the UI's generated parser cannot read.
func dropNil[T any](entries []*T) []*T {
	kept := entries[:0]
	for _, entry := range entries {
		if entry != nil {
			kept = append(kept, entry)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}
