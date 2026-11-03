package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writtenReport(t *testing.T, r *Report) map[string]any {
	t.Helper()
	path := filepath.Join(t.TempDir(), "report.json")
	require.NoError(t, r.WriteJSON(path))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))
	return decoded
}

func TestReport_ReadinessFailuresAreWrittenWhenPresent(t *testing.T) {
	decoded := writtenReport(t, &Report{
		QueueCount:        3,
		ReadinessFailures: []ReadinessFailure{{Target: "dev", Error: "canary job was not running"}},
	})
	require.Equal(t,
		[]any{map[string]any{"target": "dev", "error": "canary job was not running"}},
		decoded["readinessFailures"],
	)
}

func TestReport_ReadinessFailuresAreOmittedWhenEmpty(t *testing.T) {
	decoded := writtenReport(t, &Report{QueueCount: 3})
	require.NotContains(t, decoded, "readinessFailures")
}
