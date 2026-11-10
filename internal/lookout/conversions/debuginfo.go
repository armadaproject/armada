package conversions

import (
	"encoding/json"

	"github.com/go-openapi/strfmt"

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
func ParseDebugInfo(message string) *models.PodDebugInfo {
	info := &models.PodDebugInfo{}
	if err := json.Unmarshal([]byte(message), info); err != nil {
		return nil
	}
	if info.SchemaVersion != supportedDebugSchemaVersion {
		return nil
	}
	if err := info.Validate(strfmt.Default); err != nil {
		return nil
	}
	return info
}
