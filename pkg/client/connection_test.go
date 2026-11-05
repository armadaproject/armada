package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/client/auth/basic"
)

func TestIsLocalAddress(t *testing.T) {
	for address, want := range map[string]bool{
		"localhost":                  true,
		"localhost:8089":             true,
		"http://localhost:8089":      true,
		"https://localhost":          true,
		"127.0.0.1:8089":             true,
		"http://127.0.0.1":           true,
		"http://[::1]:8089":          true,
		"[::1]:8089":                 true,
		"lookout.example.com":        false,
		"http://lookout.example.com": false,
		"lookout.example.com:8089":   false,
		"10.0.0.1:8089":              false,
		"0.0.0.0:8089":               false,
		"localhost.evil.com":         false,
		"http://localhost.evil.com":  false,
		"http://localhost@evil.com":  false,
		"":                           false,
	} {
		assert.Equal(t, want, IsLocalAddress(address), address)
	}
}

func TestAuthHeaders(t *testing.T) {
	credentials := basic.LoginCredentials{Username: "u", Password: "p"}
	tests := map[string]struct {
		details *ApiConnectionDetails
		url     string
		want    string // "" for no headers, "headers" for headers, otherwise the error
	}{
		"no credentials need no headers":                {details: &ApiConnectionDetails{}, url: "http://lookout.example.com", want: ""},
		"credentials over plain http to a local server": {details: &ApiConnectionDetails{BasicAuth: credentials}, url: "http://localhost:8089", want: "headers"},
		"credentials over https":                        {details: &ApiConnectionDetails{BasicAuth: credentials}, url: "https://lookout.example.com", want: "headers"},
		"credentials over plain http to a remote server": {
			details: &ApiConnectionDetails{BasicAuth: credentials}, url: "http://lookout.example.com",
			want: "refusing to send credentials over plain http",
		},
		"credentials over plain http to a look-alike of a local server": {
			details: &ApiConnectionDetails{BasicAuth: credentials}, url: "http://localhost.evil.com",
			want: "refusing to send credentials over plain http",
		},
		"credentials over plain http to a remote server, when no tls is forced": {
			details: &ApiConnectionDetails{BasicAuth: credentials, ForceNoTls: true}, url: "http://lookout.example.com", want: "headers",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			headers, err := tc.details.AuthHeaders(context.Background(), tc.url)

			switch tc.want {
			case "":
				require.NoError(t, err)
				assert.Empty(t, headers)
			case "headers":
				require.NoError(t, err)
				assert.Contains(t, headers, "authorization")
			default:
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.want)
				assert.Empty(t, headers)
			}
		})
	}
}

func TestAllowsCredentialsOver(t *testing.T) {
	for _, tc := range []struct {
		name       string
		url        string
		forceNoTls bool
		want       bool
	}{
		{"https", "https://lookout.example.com", false, true},
		{"plain http to localhost", "http://localhost:8089", false, true},
		{"plain http to a loopback address", "http://127.0.0.1:8089", false, true},
		{"plain http to a remote host", "http://lookout.example.com", false, false},
		{"plain http to a look-alike of localhost", "http://localhost.evil.com", false, false},
		{"plain http to a remote host, with no tls forced", "http://lookout.example.com", true, true},
		{"an unparseable url", "http://[::1", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := &ApiConnectionDetails{ForceNoTls: tc.forceNoTls}

			assert.Equal(t, tc.want, config.AllowsCredentialsOver(tc.url))
		})
	}
}
