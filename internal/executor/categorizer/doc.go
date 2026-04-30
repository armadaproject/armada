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
//   - OnConditions: matches Kubernetes failure signals (OOMKilled, Evicted, DeadlineExceeded)
//   - OnExitCodes: matches non-zero container exit codes using In/NotIn set operators
//   - OnTerminationMessage: matches container termination messages against a regex
//   - OnPodError: matches a pod-level error message captured by the executor
//     against a regex; covers failures with no useful container terminationMessage
//     (image pull, missing volume, stuck terminating, deadline exceeded, etc.)
//
// Container-level matchers honor ContainerName scoping when set. OnPodError
// ignores it because pod-level error text has no container attribution.
//
// Exit code 0 is always skipped. Both regular and init containers are checked.
//
// # Example
//
//	application:
//	  errorCategories:
//	    enabled: true
//	    defaultCategory: "uncategorized"
//	    defaultSubcategory: "unknown"
//	    categories:
//	      - name: infrastructure
//	        rules:
//	          - onConditions: ["OOMKilled"]
//	            subcategory: "oom"
//	          - onConditions: ["Evicted"]
//	            subcategory: "eviction"
//	          - onPodError:
//	              pattern: "no match for platform in manifest"
//	            subcategory: "platform_mismatch"
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
//
//	// Terminated pod: container state carries the relevant termination signals.
//	result := classifier.ClassifyContainerError(pod)
//
//	// Pre-terminal failure: an executor-captured error message is matched
//	// against onPodError rules in addition to pod state.
//	result = classifier.ClassifyPodError(pod, podErrorMessage)
package categorizer
