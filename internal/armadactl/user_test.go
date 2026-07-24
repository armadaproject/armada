package armadactl

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/auth/basic"
	"github.com/armadaproject/armada/pkg/client/auth/oidc"
)

func TestActionUser(t *testing.T) {
	for name, tc := range map[string]struct {
		config *client.ApiConnectionDetails
		want   string
	}{
		"basic auth username": {
			config: &client.ApiConnectionDetails{BasicAuth: basic.LoginCredentials{Username: "alice"}},
			want:   "alice",
		},
		"openid password username": {
			config: &client.ApiConnectionDetails{OpenIdPasswordAuth: oidc.ClientPasswordDetails{Username: "bob"}},
			want:   "bob",
		},
		"anonymous without auth": {
			config: &client.ApiConnectionDetails{},
			want:   "anonymous",
		},
		"unknown for device auth": {
			config: &client.ApiConnectionDetails{OpenIdDeviceAuth: oidc.DeviceDetails{ProviderUrl: "https://issuer.example"}},
			want:   "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			app := &App{Params: &Params{ApiConnectionDetails: tc.config}}

			assert.Equal(t, tc.want, app.actionUser())
		})
	}
}

func TestPrintActionUser(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &App{
		Params: &Params{ApiConnectionDetails: &client.ApiConnectionDetails{}},
		Out:    buf,
	}

	app.printActionUser()

	assert.Equal(t, "user: anonymous\n", buf.String())
}
