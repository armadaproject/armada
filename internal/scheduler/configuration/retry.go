package configuration

// RetryPolicyConfig controls the scheduler's retry policy behavior.
type RetryPolicyConfig struct {
	// Enabled controls whether the retry policy engine is active.
	Enabled bool `yaml:"enabled"`
	// GlobalMaxRetries is the hard upper limit on retries across all policies.
	GlobalMaxRetries uint `yaml:"globalMaxRetries"`
}
