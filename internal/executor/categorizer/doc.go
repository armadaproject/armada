// Package categorizer classifies pod failures into a single named category
// with an optional subcategory, based on configurable rules. It runs at the
// executor, where full Kubernetes pod status is available. The resulting
// category and subcategory are set on the Error proto attached to events.
//
// # Configuration
//
// Categories are defined in the executor config under application.errorCategories.
// Each category has a name and one or more rules. The classifier evaluates
// rules in config order across all categories. The first matching rule wins
// and sets both the category name and the rule's optional subcategory.
//
// Each rule uses exactly one matcher:
//   - [CategoryRule.OnConditions]: matches Kubernetes failure signals (OOMKilled, Evicted, DeadlineExceeded)
//   - [CategoryRule.OnExitCodes]: matches non-zero container exit codes using In/NotIn set operators
//   - [CategoryRule.OnTerminationMessage]: matches container termination messages against a regex
//   - [CategoryRule.OnPodError]: matches a regex against the pod-level error
//     message the executor captured. It covers failures with no useful
//     container terminationMessage (image pull, missing volume, stuck
//     terminating, etc.)
//   - [CategoryRule.OnPodEvents]: matches the pod's Kubernetes events, where
//     kubelet admission and device-plugin failures appear most reliably
//
// Container-level matchers honor [CategoryRule.ContainerName] scoping when
// set. OnPodError and OnPodEvents ignore it because pod-level failures have
// no container attribution.
//
// Each rule may also set [CategoryRule.Hint], an optional user-facing string
// that the executor appends to the failure message. Hints go into
// lookoutdb.job_run.error, and Lookout shows them to users alongside the raw
// runtime error.
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
//	            hint: "Increase the memory request in your job spec"
//	          - onConditions: ["Evicted"]
//	            subcategory: "eviction"
//	          - onPodError:
//	              pattern: "no match for platform in manifest"
//	            subcategory: "platform_mismatch"
//	            hint: "Build the image for the cluster's CPU architecture (typically x64/arm64 mismatch)"
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
//	// Pod-level failure: an executor-captured error message and the pod's
//	// Kubernetes events are matched against onPodError and onPodEvents rules
//	// in addition to pod state. Pass nil events when they are unavailable.
//	result = classifier.ClassifyPodError(pod, podErrorMessage, podEvents)
package categorizer
