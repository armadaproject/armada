package diagnostics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLookupHint(t *testing.T) {
	tests := map[string]struct {
		message       string
		wantSubstring string
	}{
		"empty message returns no hint": {
			message: "",
		},
		"unknown failure returns no hint": {
			message: "Some unrelated kubelet message",
		},
		"platform mismatch matches": {
			message:       `Failed to pull image "amd64/busybox:latest": rpc error: code = NotFound desc = failed to pull and unpack image "docker.io/amd64/busybox:latest": no match for platform in manifest: not found`,
			wantSubstring: "x64/arm64",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := LookupHint(tc.message)
			if tc.wantSubstring == "" {
				assert.Empty(t, got)
				return
			}
			assert.Contains(t, got, tc.wantSubstring)
		})
	}
}
