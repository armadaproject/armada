package categorizer

import "github.com/armadaproject/armada/internal/common/errormatch"

// ErrorCategoriesConfig is the top-level config for failure classification.
type ErrorCategoriesConfig struct {
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
}

// CategoryRule defines a single matching condition. Exactly one matcher must
// be set per rule (validated by NewClassifier). Rules within a category are OR'd.
//
// Container-level matchers (OnConditions, OnExitCodes, OnTerminationMessage)
// inspect per-container state from pod.Status; ContainerName scopes them to a
// specific container when set, otherwise any container can match.
//
// OnPodError is pod-level: it matches the kubelet/runtime error captured for
// failures that happen before or around startup (image pull, missing volume,
// missing config), where no container has terminated with a usable
// terminationMessage. ContainerName is ignored for OnPodError because the
// captured text has no container attribution.
type CategoryRule struct {
	ContainerName        string                      `yaml:"containerName,omitempty"`
	OnExitCodes          *errormatch.ExitCodeMatcher `yaml:"onExitCodes,omitempty"`
	OnTerminationMessage *errormatch.RegexMatcher    `yaml:"onTerminationMessage,omitempty"`
	OnPodError           *errormatch.RegexMatcher    `yaml:"onPodError,omitempty"`
	OnConditions         []string                    `yaml:"onConditions,omitempty"`
	Subcategory          string                      `yaml:"subcategory,omitempty"`
}
