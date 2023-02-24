package pulsarutils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

func TestCreatePulsarClientHappyPath(t *testing.T) {
	cwd, _ := os.Executable() // Need a valid directory for tokens and certs

	// test with auth and tls configured
	config := &configuration.PulsarConfig{
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
	config = &configuration.PulsarConfig{
		URL:                     "pulsar://pulsarhost:50000",
		MaxConnectionsPerBroker: 100,
	}
	_, err = NewPulsarClient(config)
	assert.NoError(t, err)
}

func TestCreatePulsarClientInvalidAuth(t *testing.T) {
	// No Auth type
	_, err := NewPulsarClient(&configuration.PulsarConfig{
		AuthenticationEnabled: true,
	})
	assert.Error(t, err)

	// Invalid Auth type
	_, err = NewPulsarClient(&configuration.PulsarConfig{
		AuthenticationEnabled: true,
		AuthenticationType:    "INVALID",
	})
	assert.Error(t, err)

	// No Token
	_, err = NewPulsarClient(&configuration.PulsarConfig{
		AuthenticationEnabled: true,
		AuthenticationType:    "JWT",
	})
	assert.Error(t, err)
}
