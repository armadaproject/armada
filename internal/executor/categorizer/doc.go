// Package categorizer classifies pod failures into a single named category
// with an optional subcategory, based on configurable rules. It runs at the
// executor, where full Kubernetes pod status is available. The resulting
// category and subcategory are set on the Error proto attached to events.
//
// # Configuration
//
// Categories are defined in the executor config under application.errorCategories.
// Each category has a name and one or more rules. Rules are evaluated in config
// order across all categories; the first matching rule wins, setting both the
// category name and the rule's optional subcategory.
//
// Each rule uses exactly one matcher:
//   - OnConditions: matches Kubernetes failure signals (OOMKilled, Evicted, DeadlineExceeded, AppError)
//   - OnExitCodes: matches non-zero container exit codes using In/NotIn set operators
//   - OnTerminationMessage: matches container termination messages against a regex
//
// Exit code 0 is always skipped. Both regular and init containers are checked.
//
// # Example
//
//	application:
//	  errorCategories:
//	    defaultCategory: "uncategorized"
//	    defaultSubcategory: "unknown"
//	    categories:
//	      - name: infrastructure
//	        rules:
//	          - onConditions: ["OOMKilled"]
//	            subcategory: "oom"
//	          - onConditions: ["Evicted"]
//	            subcategory: "eviction"
//	      - name: user_code
//	        rules:
//	          - onExitCodes:
//	              operator: In
//	              values: [74, 75]
//	            subcategory: "cuda"
//	          - onTerminationMessage:
//	              pattern: "(?i)cuda.*error"
//	            subcategory: "cuda"
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
//	result := classifier.Classify(pod) // result.Category = "infrastructure", result.Subcategory = "oom"
package categorizer
