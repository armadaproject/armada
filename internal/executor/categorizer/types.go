package categorizer

import "github.com/armadaproject/armada/internal/common/errormatch"

// ErrorCategoriesConfig is the top-level config for failure classification.
type ErrorCategoriesConfig struct {
	// Enabled toggles failure classification on pod errors. When false, no
	// failure_category or failure_subcategory is set on error events.
	Enabled bool `yaml:"enabled"`
	// DefaultCategory is the category assigned when no rule matches.
	// If empty, no category is assigned when no rule matches.
	DefaultCategory string `yaml:"defaultCategory"`
	// DefaultSubcategory is the subcategory assigned when no rule matches.
	// If empty, no subcategory is assigned when no rule matches.
	DefaultSubcategory string           `yaml:"defaultSubcategory"`
	Categories         []CategoryConfig `yaml:"categories"`
}

// CategoryConfig defines a named error category with rules that match against
// pod failure signals. The first matching rule (across all categories, in config order)
// wins - setting both the category name and the rule's optional subcategory.
type CategoryConfig struct {
	Name  string         `yaml:"name"`
	Rules []CategoryRule `yaml:"rules"`
	// Action is what the executor does with a failed pod in this category.
	// Empty (the default) means Retain.
	Action PodFailureAction `yaml:"action"`
}

// PodFailureAction is the executor's disposition of a failed pod in a category.
type PodFailureAction string

const (
	// PodFailureActionRetain keeps the failed pod in place until normal GC, so
	// its logs remain available for debugging.
	PodFailureActionRetain PodFailureAction = "Retain"
	// PodFailureActionDelete deletes the failed pod and reports the failure
	// once the pod is gone, freeing the pod name for a retry of the same job.
	// Whether the job is retried stays with the scheduler's retry policy.
	// Intended for narrow infrastructure-failure categories replacing failed
	// pod checks, not for broad categories like user errors.
	PodFailureActionDelete PodFailureAction = "Delete"
)

// CategoryRule defines a single matching condition. Exactly one matcher must
// be set per rule (validated by NewClassifier). Rules within a category are OR'd.
//
// Container-level matchers (OnConditions, OnExitCodes, OnTerminationMessage)
// inspect per-container state from pod.Status; ContainerName scopes them to a
// specific container when set, otherwise any container can match.
//
// OnPodError is pod-level: it matches a regex against the failure message that
// the executor reports for the run. For a failed pod, the message is the pod
// status message, or else one line for each failed container with its exit
// code and termination message. So a rule can also match text that the
// program wrote. Use it for failures where no container has a useful
// terminationMessage, for example kubelet and runtime errors (image pull,
// missing volume, missing config). ContainerName is ignored for OnPodError
// because the message has no container attribution.
//
// OnPodEvents is also pod-level: it matches the Kubernetes events of the pod
// when the executor detects the failure. Events carry failures that often do
// not reach pod or container status, for example kubelet admission and
// device-plugin errors. The kubelet writes events asynchronously, so an event
// that arrives after the executor detects the failure does not count.
// ContainerName is ignored for OnPodEvents.
//
// Armada gives the failures that it detects itself, for example stuck
// terminating or externally deleted, the built-in category internal. The
// classifier does not evaluate rules for them.
type CategoryRule struct {
	ContainerName        string                      `yaml:"containerName,omitempty"`
	OnExitCodes          *errormatch.ExitCodeMatcher `yaml:"onExitCodes,omitempty"`
	OnTerminationMessage *errormatch.RegexMatcher    `yaml:"onTerminationMessage,omitempty"`
	OnPodError           *errormatch.RegexMatcher    `yaml:"onPodError,omitempty"`
	OnPodEvents          *errormatch.PodEventMatcher `yaml:"onPodEvents,omitempty"`
	OnConditions         []string                    `yaml:"onConditions,omitempty"`
	Subcategory          string                      `yaml:"subcategory,omitempty"`
	// Hint is operator-supplied user-facing copy describing this failure mode.
	// When set, it is appended to the failure message that lands in
	// lookoutdb.job_run.error so end users see actionable guidance alongside the
	// raw runtime error. Optional; empty means no hint is added.
	Hint string `yaml:"hint,omitempty"`
}
