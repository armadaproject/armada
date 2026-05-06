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
}

// CategoryRule defines a single matching condition. Exactly one matcher must
// be set per rule (validated by NewClassifier). Rules within a category are OR'd.
// When ContainerName is set, only failures from that container are considered.
// When empty, failures from any container can match (default).
type CategoryRule struct {
	ContainerName        string                      `yaml:"containerName,omitempty"`
	OnExitCodes          *errormatch.ExitCodeMatcher `yaml:"onExitCodes,omitempty"`
	OnTerminationMessage *errormatch.RegexMatcher    `yaml:"onTerminationMessage,omitempty"`
	OnConditions         []string                    `yaml:"onConditions,omitempty"`
	Subcategory          string                      `yaml:"subcategory,omitempty"`
}
