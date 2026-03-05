package categorizer

import "github.com/armadaproject/armada/internal/common/errormatch"

// CategoryConfig defines a named error category with rules that match against
// pod failure signals. When any rule matches, the category name is included
// in the FailureInfo.categories field of the error event.
type CategoryConfig struct {
	Name  string         `yaml:"name"`
	Rules []CategoryRule `yaml:"rules"`
}

// CategoryRule defines a single matching condition. Exactly one matcher must
// be set per rule (validated by NewClassifier). Rules within a category are OR'd.
type CategoryRule struct {
	OnExitCodes          *errormatch.ExitCodeMatcher `yaml:"onExitCodes,omitempty"`
	OnTerminationMessage *errormatch.RegexMatcher    `yaml:"onTerminationMessage,omitempty"`
	OnConditions         []string                    `yaml:"onConditions,omitempty"`
}
