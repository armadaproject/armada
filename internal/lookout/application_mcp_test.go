package lookout

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookout/configuration"
	"github.com/armadaproject/armada/internal/lookoutmcp/mcp"
)

func testLogger() *logging.Logger {
	zl := zerolog.New(io.Discard)
	return logging.FromZerolog(zl)
}

func TestValidateJobID(t *testing.T) {
	tests := []struct {
		name    string
		jobID   string
		wantErr bool
	}{
		{"valid alphanumeric", "job123", false},
		{"valid with dash", "job-123", false},
		{"valid with underscore", "job_123", false},
		{"empty", "", true},
		{"too long", string(make([]byte, 129)), true},
		{"invalid characters", "job@123", true},
		{"valid ulid", "01f3j0g1md4qx7z5qb148qnh4d", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateJobID(tt.jobID)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExtractJobID(t *testing.T) {
	tests := []struct {
		name    string
		args    map[string]any
		want    string
		wantErr bool
	}{
		{
			name:    "valid job ID",
			args:    map[string]any{"jobId": "test123"},
			want:    "test123",
			wantErr: false,
		},
		{
			name:    "missing job ID",
			args:    map[string]any{},
			want:    "",
			wantErr: true,
		},
		{
			name:    "non-string job ID",
			args:    map[string]any{"jobId": 123},
			want:    "",
			wantErr: true,
		},
		{
			name:    "invalid job ID",
			args:    map[string]any{"jobId": "job@123"},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractJobID(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestValidateRunID(t *testing.T) {
	tests := []struct {
		name    string
		runID   string
		wantErr bool
	}{
		{"valid", "run123", false},
		{"empty", "", true},
		{"too long", string(make([]byte, 129)), true},
		{"valid uuid", "550e8400-e29b-41d4-a716-446655440000", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRunID(tt.runID)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMCPInitialize(t *testing.T) {
	initRequest := mcp.Request{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
		Params: json.RawMessage(`{
			"protocolVersion": "2024-11-05",
			"capabilities": {},
			"clientInfo": {"name": "test", "version": "1.0"}
		}`),
	}

	reqBody, err := json.Marshal(initRequest)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(append(reqBody, '\n')))
	w := httptest.NewRecorder()

	logger := testLogger()
	state := &mcpServerState{
		config: configuration.LookoutConfig{},
		logger: logger,
	}

	ctx := req.Context()
	err = state.handleMCPRequest(*armadacontext.New(ctx, logger), reqBody, w)
	require.NoError(t, err)

	var response mcp.Response
	err = json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "2.0", response.JSONRPC)
	assert.Nil(t, response.Error)
	assert.NotNil(t, response.Result)
}

func TestMCPListTools(t *testing.T) {
	listToolsRequest := mcp.Request{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "tools/list",
	}

	reqBody, err := json.Marshal(listToolsRequest)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(append(reqBody, '\n')))
	w := httptest.NewRecorder()

	logger := testLogger()
	state := &mcpServerState{
		config: configuration.LookoutConfig{},
		logger: logger,
	}

	ctx := req.Context()
	err = state.handleMCPRequest(*armadacontext.New(ctx, logger), reqBody, w)
	require.NoError(t, err)

	var response mcp.Response
	err = json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "2.0", response.JSONRPC)
	assert.Nil(t, response.Error)
	assert.NotNil(t, response.Result)

	resultBytes, err := json.Marshal(response.Result)
	require.NoError(t, err)

	var listToolsResult mcp.ListToolsResult
	err = json.Unmarshal(resultBytes, &listToolsResult)
	require.NoError(t, err)

	expectedTools := []string{
		"search_jobs",
		"get_job_details",
		"get_job_spec",
		"get_job_error",
		"get_job_run_error",
		"get_job_run_debug",
		"get_job_commands",
		"get_job_links",
		"get_lookout_ui_link",
	}

	assert.Len(t, listToolsResult.Tools, len(expectedTools))

	toolNames := make([]string, len(listToolsResult.Tools))
	for i, tool := range listToolsResult.Tools {
		toolNames[i] = tool.Name
	}

	for _, expectedTool := range expectedTools {
		assert.Contains(t, toolNames, expectedTool, "Tool %s should be present", expectedTool)
	}
}

func TestMCPInvalidMethod(t *testing.T) {
	invalidRequest := mcp.Request{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "invalid/method",
	}

	reqBody, err := json.Marshal(invalidRequest)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(append(reqBody, '\n')))
	w := httptest.NewRecorder()

	logger := testLogger()
	state := &mcpServerState{
		config: configuration.LookoutConfig{},
		logger: logger,
	}

	ctx := req.Context()
	err = state.handleMCPRequest(*armadacontext.New(ctx, logger), reqBody, w)
	require.NoError(t, err)

	var response mcp.Response
	err = json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "2.0", response.JSONRPC)
	assert.NotNil(t, response.Error)
	assert.Equal(t, mcp.ErrorCodeMethodNotFound, response.Error.Code)
}

func TestMCPParseError(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader([]byte("invalid json")))
	w := httptest.NewRecorder()

	logger := testLogger()
	state := &mcpServerState{
		config: configuration.LookoutConfig{},
		logger: logger,
	}

	ctx := req.Context()
	err := state.handleMCPRequest(*armadacontext.New(ctx, logger), []byte("invalid json"), w)
	assert.Error(t, err)

	var response mcp.Response
	err = json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	assert.Equal(t, "2.0", response.JSONRPC)
	assert.NotNil(t, response.Error)
	assert.Equal(t, mcp.ErrorCodeParseError, response.Error.Code)
}
