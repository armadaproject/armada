package lookout

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-openapi/runtime"

	"github.com/armadaproject/armada/internal/lookout/gen/restapi/operations"
)

func TestVersionHandler_ReturnsExpectedJSON(t *testing.T) {
	responder := versionHandler("v1.2.3", "abc1234", "2026-05-14T10:30:00Z")(operations.GetVersionParams{})

	rec := httptest.NewRecorder()
	responder.WriteResponse(rec, runtime.JSONProducer())

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d", rec.Code, http.StatusOK)
	}

	var payload struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		BuildTime string `json:"buildTime"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if payload.Version != "v1.2.3" {
		t.Errorf("version = %q, want %q", payload.Version, "v1.2.3")
	}
	if payload.Commit != "abc1234" {
		t.Errorf("commit = %q, want %q", payload.Commit, "abc1234")
	}
	if payload.BuildTime != "2026-05-14T10:30:00Z" {
		t.Errorf("buildTime = %q, want %q", payload.BuildTime, "2026-05-14T10:30:00Z")
	}
}

func TestVersionHandler_PassesThroughDevDefaults(t *testing.T) {
	responder := versionHandler("dev", "unknown", "unknown")(operations.GetVersionParams{})

	rec := httptest.NewRecorder()
	responder.WriteResponse(rec, runtime.JSONProducer())

	var payload struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		BuildTime string `json:"buildTime"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if payload.Version != "dev" || payload.Commit != "unknown" || payload.BuildTime != "unknown" {
		t.Errorf("got %+v, want defaults", payload)
	}
}
