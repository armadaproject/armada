package pulsarutils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	commonconfig "github.com/armadaproject/armada/internal/common/config"
)

func TestCreatePulsarClientHappyPath(t *testing.T) {
	cwd, _ := os.Executable() // Need a valid directory for tokens and certs

	// test with auth and tls configured
	config := &commonconfig.PulsarConfig{
		URL: "pulsar://pulsarhost:50000",

		TLSTrustCertsFilePath:      cwd,
		TLSAllowInsecureConnection: true,
		TLSValidateHostname:        true,
		MaxConnectionsPerBroker:    100,
		AuthenticationEnabled:      true,
		AuthenticationType:         "JWT",
		JwtTokenPath:               cwd,
	}
	_, err := NewPulsarClient(config)
	assert.NoError(t, err)

	// Test without auth or TLS
	config = &commonconfig.PulsarConfig{
		URL:                     "pulsar://pulsarhost:50000",
		MaxConnectionsPerBroker: 100,
	}
	_, err = NewPulsarClient(config)
	assert.NoError(t, err)
}

func TestCreatePulsarClientInvalidAuth(t *testing.T) {
	// No Auth type
	_, err := NewPulsarClient(&commonconfig.PulsarConfig{
		AuthenticationEnabled: true,
	})
	assert.Error(t, err)

	// Invalid Auth type
	_, err = NewPulsarClient(&commonconfig.PulsarConfig{
		AuthenticationEnabled: true,
		AuthenticationType:    "INVALID",
	})
	assert.Error(t, err)

	// No Token
	_, err = NewPulsarClient(&commonconfig.PulsarConfig{
		AuthenticationEnabled: true,
		AuthenticationType:    "JWT",
	})
	assert.Error(t, err)
}
