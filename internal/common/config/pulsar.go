package config

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarConfig struct {
	// Pulsar URL
	URL string
	// Pulsar REST API URL (Pulsar admin API)
	// If not set, event latency metrics will not be published
	RestURL string
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
	// The config for topic processing delay monitor
	DelayMonitor TopicDelayMonitor
	// The pulsar topic that Jobset Events will be published to
	JobsetEventsTopic string
	// The pulsar topic that Control Plane Events will be published to
	ControlPlaneEventsTopic string
	// The pulsar topic that Metric Events will be published to
	MetricEventsTopic string
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
	// Initial backoff in the exponential-with-jitter backoff sequence used when polling or
	// retrying fails.
	BackoffTime time.Duration
	// Upper bound on the backoff duration between retries. If unset or <= 0, defaults to
	// BackoffTime (i.e. no growth).
	MaxBackoffTime time.Duration
	// Fraction by which each backoff is randomised: the actual wait is drawn uniformly from
	// [interval * (1 - BackoffRandomizationFactor), interval * (1 + BackoffRandomizationFactor)].
	// If unset or < 0, defaults to backoff.DefaultRandomizationFactor (0.5).
	BackoffRandomizationFactor float64
	// Factor by which the backoff interval grows after each retry. If unset or <= 0, defaults to
	// backoff.DefaultMultiplier (1.5).
	BackoffMultiplier float64
	// Number of pulsar messages that will be queued by the pulsar consumer.
	ReceiverQueueSize int
	// The pulsar topic that messages will be published to if a sink cannot store them after DeadLetterMaxAttempts attempts
	DeadLetterTopic string
	// Number of consecutive Sink.Store attempts before a message is published to DeadLetterTopic and acked.
	// If set, must be at least 2: a value of 1 would dead-letter on the first failure with no retry at all.
	DeadLetterMaxAttempts int `validate:"omitempty,gte=2"`
}

// Validate checks invariants that span multiple fields and so cannot be expressed via struct tags alone.
func (c PulsarConfig) Validate() error {
	if c.MaxBackoffTime > 0 && c.MaxBackoffTime < c.BackoffTime {
		return fmt.Errorf("pulsar.maxBackoffTime (%s) must be >= pulsar.backoffTime (%s) if set", c.MaxBackoffTime, c.BackoffTime)
	}
	return nil
}

type TopicDelayMonitor struct {
	// If the topic processing delay component should be enabled
	// When enabled we'll expose metrics that show the processing delay for each partition
	Enabled bool
	// How often the monitor will check the delay for a partition
	Interval time.Duration
}
