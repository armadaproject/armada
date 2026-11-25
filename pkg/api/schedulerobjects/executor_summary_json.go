package schedulerobjects

import (
	"encoding/json"
	"time"

	types "github.com/gogo/protobuf/types"
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

// UnmarshalJSON reads what MarshalJSON writes, so that Go clients can decode the response of the REST gateway.
func (m *ExecutorSummary) UnmarshalJSON(data []byte) error {
	in := struct {
		Name           string `json:"name,omitempty"`
		Pool           string `json:"pool,omitempty"`
		LastUpdateTime string `json:"lastUpdateTime,omitempty"`
		NodeCount      int32  `json:"nodeCount,omitempty"`
		Cordoned       bool   `json:"cordoned,omitempty"`
		CordonReason   string `json:"cordonReason,omitempty"`
		CordonedBy     string `json:"cordonedBy,omitempty"`
	}{}
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	*m = ExecutorSummary{
		Name:         in.Name,
		Pool:         in.Pool,
		NodeCount:    in.NodeCount,
		Cordoned:     in.Cordoned,
		CordonReason: in.CordonReason,
		CordonedBy:   in.CordonedBy,
	}
	if in.LastUpdateTime != "" {
		t, err := time.Parse(time.RFC3339Nano, in.LastUpdateTime)
		if err != nil {
			return err
		}
		m.LastUpdateTime = &types.Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
	}
	return nil
}
