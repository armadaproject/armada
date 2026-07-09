package configuration

// RetryPolicyConfig controls the scheduler's retry policy behavior.
type RetryPolicyConfig struct {
	// Enabled controls whether the retry policy engine is active.
	Enabled bool `yaml:"enabled"`
	// GlobalMaxRetries is the scheduler-wide cap on genuine-failure retries per
	// job, on top of every policy. It counts retries, not runs: the initial
	// failure consumes no budget, only subsequent retries do. Preempted and
	// lease-returned runs are never charged. A value of 0 is the kill switch:
	// no job is ever retried by the policy engine. There is no unlimited
	// setting for the global cap.
	GlobalMaxRetries uint `yaml:"globalMaxRetries"`
	// DefaultPolicyName is the retry policy applied to jobs whose queue has no
	// policy of its own. It lets an operator turn on retry policies fleet-wide
	// with a single named policy before per-queue attachment is configured.
	// Optional: when empty, only queues with an attached policy get engine
	// decisions and every other queue keeps legacy behaviour.
	DefaultPolicyName string `yaml:"defaultPolicyName"`
}
