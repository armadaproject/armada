package pulsarutils

import (
	"os"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

func TestParsePulsarCompressionType(t *testing.T) {
	// No compression
	comp, err := ParsePulsarCompressionType("")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.NoCompression, comp)

	// Zlib
	comp, err = ParsePulsarCompressionType("ZliB")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.ZLib, comp)

	// Zstd
	comp, err = ParsePulsarCompressionType("zstd")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.ZSTD, comp)

	// Lz4
	comp, err = ParsePulsarCompressionType("LZ4")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.LZ4, comp)

	// unknown
	_, err = ParsePulsarCompressionType("not a valid compression")
	assert.Error(t, err)
}

func TestParsePulsarCompressionLevel(t *testing.T) {
	// No compression
	comp, err := ParsePulsarCompressionLevel("")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.Default, comp)

	comp, err = ParsePulsarCompressionLevel("Default")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.Default, comp)

	// Faster
	comp, err = ParsePulsarCompressionLevel("FASTER")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.Faster, comp)

	// Better
	comp, err = ParsePulsarCompressionLevel("Better")
	assert.NoError(t, err)
	assert.Equal(t, pulsar.Better, comp)

	// unknown
	_, err = ParsePulsarCompressionLevel("not a valid compression type")
	assert.Error(t, err)
}

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
