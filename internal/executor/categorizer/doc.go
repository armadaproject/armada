// Package categorizer classifies pod failures into named categories based
// on configurable rules. It runs at the executor, where full Kubernetes pod
// status is available. The resulting category names are included in the
// FailureInfo proto attached to error events.
//
// # Configuration
//
// Categories are defined in the executor config under application.errorCategories.
// Each category has a name and one or more rules. Rules within a category are
// OR'd: if any rule matches, the category is included. A pod can match multiple
// categories.
//
// Each rule uses exactly one matcher:
//   - OnConditions: matches Kubernetes failure signals (OOMKilled, Evicted, DeadlineExceeded)
//   - OnExitCodes: matches non-zero container exit codes using In/NotIn set operators
//   - OnTerminationMessage: matches container termination messages against a regex
//
// Exit code 0 is always skipped. Both regular and init containers are checked.
//
// # Example
//
//	application:
//	  errorCategories:
//	    - name: oom
//	      rules:
//	        - onConditions: ["OOMKilled"]
//	    - name: cuda_error
//	      rules:
//	        - onExitCodes:
//	            operator: In
//	            values: [74, 75]
//	        - onTerminationMessage:
//	            pattern: "(?i)cuda.*error"
//	    - name: transient_infra
//	      rules:
//	        - onConditions: ["Evicted"]
//	        - onExitCodes:
//	            operator: In
//	            values: [137]
//
// # Validation
//
// [NewClassifier] validates all config upfront: unknown condition strings,
// invalid exit code operators, empty value lists, and invalid regexes all
// return errors at construction time.
//
// # Usage
//
//	classifier, err := categorizer.NewClassifier(config.ErrorCategories)
//	if err != nil {
//	    // handle invalid config
//	}
//	categories := classifier.Classify(pod) // returns []string{"oom", "cuda_error"} or nil
package categorizer
