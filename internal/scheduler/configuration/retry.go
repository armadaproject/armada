package configuration

// RetryPolicyConfig controls the scheduler's retry policy behavior.
type RetryPolicyConfig struct {
	// Enabled controls whether the retry policy engine is active.
	Enabled bool `yaml:"enabled"`
	// GlobalMaxRetries is the maximum number of retries per job across all
	// policies. It is counted in retries (not failures): the initial failure
	// does not consume budget, only subsequent re-leases do. A value of 0
	// disables the global cap; per-policy retryLimit still applies.
	GlobalMaxRetries uint `yaml:"globalMaxRetries"`
}
