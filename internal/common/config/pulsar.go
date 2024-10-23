package config

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarConfig struct {
	// Pulsar URL
	URL string `validate:"required"`
	// Path to the trusted TLS certificate file (must exist)
	TLSTrustCertsFilePath string
	// Whether Pulsar client accept untrusted TLS certificate from broker
	TLSAllowInsecureConnection bool
	// Whether the Pulsar client will validate the hostname in the broker's TLS Cert matches the actual hostname.
	TLSValidateHostname bool
	// Max number of connections to a single broker that will be kept in the pool. (Default: 1 connection)
	MaxConnectionsPerBroker int
	// Whether Pulsar authentication is enabled
	AuthenticationEnabled bool
	// Authentication type. For now only "JWT" auth is valid
	AuthenticationType string
	// Path to the JWT token (must exist). This must be set if AuthenticationType is "JWT"
	JwtTokenPath string
	// The pulsar topic that Jobset Events will be published to
	JobsetEventsTopic string
	// The pulsar topic that Control Plane Events will be published to
	ControlPlaneEventsTopic string
	// Compression to use.  Valid values are "None", "LZ4", "Zlib", "Zstd".  Default is "None"
	CompressionType pulsar.CompressionType
	// Compression Level to use.  Valid values are "Default", "Better", "Faster".  Default is "Default"
	CompressionLevel pulsar.CompressionLevel
	// Maximum allowed Events per message
	MaxAllowedEventsPerMessage int `validate:"gte=0"`
	// Maximum allowed message size in bytes
	MaxAllowedMessageSize uint
	// Timeout when sending messages asynchronously
	SendTimeout time.Duration
	// Backoff from polling when Pulsar returns an error
	BackoffTime time.Duration
	// Number of pulsar messages that will be queued by the pulsar consumer.
	ReceiverQueueSize int
}
