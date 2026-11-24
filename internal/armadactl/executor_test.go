package armadactl

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
)

var testExecutors = []*schedulerobjects.ExecutorSummary{
	{
		Name:           "executor-a",
		Pool:           "cpu",
		LastUpdateTime: protoutil.ToTimestamp(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)),
		NodeCount:      3,
	},
	{
		Name:         "executor-b",
		Pool:         "gpu",
		Cordoned:     true,
		CordonReason: "maintenance",
		CordonedBy:   "alice",
	},
}

func TestGetAllExecutors_Table(t *testing.T) {
	a, out := newTestApp()
	a.Params.ExecutorAPI.Get = func(name string) ([]*schedulerobjects.ExecutorSummary, error) {
		assert.Empty(t, name)
		return testExecutors, nil
	}

	require.NoError(t, a.GetAllExecutors())

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	require.Len(t, lines, 3)
	assert.Contains(t, lines[0], "NAME")
	assert.Contains(t, lines[1], "executor-a")
	assert.Contains(t, lines[1], "2026-11-24T10:00:00Z")
	assert.Contains(t, lines[1], "false")
	assert.Contains(t, lines[2], "executor-b")
	assert.Contains(t, lines[2], "maintenance")
	assert.Contains(t, lines[2], "alice")
}

func TestGetAllExecutors_Empty(t *testing.T) {
	a, out := newTestApp()
	a.Params.ExecutorAPI.Get = func(string) ([]*schedulerobjects.ExecutorSummary, error) {
		return nil, nil
	}

	require.NoError(t, a.GetAllExecutors())
	assert.Equal(t, "No executors found\n", out.String())
}

func TestGetAllExecutors_Yaml(t *testing.T) {
	a, out := newTestApp()
	a.Params.Output = OutputYaml
	a.Params.ExecutorAPI.Get = func(string) ([]*schedulerobjects.ExecutorSummary, error) {
		return testExecutors, nil
	}

	require.NoError(t, a.GetAllExecutors())

	var doc struct {
		Kind      string         `json:"kind"`
		Executors []executorInfo `json:"executors"`
	}
	require.NoError(t, yaml.Unmarshal(out.Bytes(), &doc))
	assert.Equal(t, "Executor", doc.Kind)
	require.Len(t, doc.Executors, 2)
	assert.Equal(t, "executor-a", doc.Executors[0].Name)
	require.NotNil(t, doc.Executors[0].LastUpdateTime)
	assert.True(t, doc.Executors[0].LastUpdateTime.Equal(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)))
	assert.Nil(t, doc.Executors[1].LastUpdateTime)
	assert.True(t, doc.Executors[1].Cordoned)
	assert.Equal(t, "maintenance", doc.Executors[1].CordonReason)
}

func TestGetExecutor_Json(t *testing.T) {
	a, out := newTestApp()
	a.Params.Output = OutputJson
	a.Params.ExecutorAPI.Get = func(name string) ([]*schedulerobjects.ExecutorSummary, error) {
		assert.Equal(t, "executor-b", name)
		return testExecutors[1:], nil
	}

	require.NoError(t, a.GetExecutor("executor-b"))

	var doc map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &doc))
	assert.Equal(t, "Executor", doc["kind"])
	assert.Equal(t, "executor-b", doc["name"])
	assert.Equal(t, true, doc["cordoned"])
	assert.Equal(t, "alice", doc["cordonedBy"])
}

func TestGetExecutor_Error(t *testing.T) {
	a, _ := newTestApp()
	a.Params.ExecutorAPI.Get = func(string) ([]*schedulerobjects.ExecutorSummary, error) {
		return nil, fmt.Errorf("rpc error: code = NotFound desc = executor missing not found")
	}

	err := a.GetExecutor("missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error getting executor missing")
}
