package schedulerobjects

import (
	"encoding/json"
	"time"
)

// MarshalJSON writes the heartbeat as an RFC 3339 string, which is what the OpenAPI schema promises. Without
// this the gateway would write the protobuf timestamp as an object with seconds and nanos.
func (m *ExecutorSummary) MarshalJSON() ([]byte, error) {
	out := struct {
		Name           string `json:"name,omitempty"`
		Pool           string `json:"pool,omitempty"`
		LastUpdateTime string `json:"lastUpdateTime,omitempty"`
		NodeCount      int32  `json:"nodeCount,omitempty"`
		Cordoned       bool   `json:"cordoned,omitempty"`
		CordonReason   string `json:"cordonReason,omitempty"`
		CordonedBy     string `json:"cordonedBy,omitempty"`
	}{
		Name:         m.Name,
		Pool:         m.Pool,
		NodeCount:    m.NodeCount,
		Cordoned:     m.Cordoned,
		CordonReason: m.CordonReason,
		CordonedBy:   m.CordonedBy,
	}
	if t := m.LastUpdateTime; t != nil {
		out.LastUpdateTime = time.Unix(t.Seconds, int64(t.Nanos)).UTC().Format(time.RFC3339Nano)
	}
	return json.Marshal(out)
}
