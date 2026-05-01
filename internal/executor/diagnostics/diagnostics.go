package diagnostics

import "regexp"

type hint struct {
	pattern *regexp.Regexp
	text    string
}

// builtinHints is consulted in order; first match wins, so place
// more-specific patterns before broader ones.
var builtinHints = []hint{
	{
		pattern: regexp.MustCompile(`no match for platform in manifest`),
		text:    "No compatible image was found for the target farm node's architecture; this is often due to an x64/arm64 mismatch, so check the image architecture or build and publish a compatible version.",
	},
}

// LookupHint returns user-facing guidance matching the failure message,
// or "" if no built-in hint applies.
func LookupHint(message string) string {
	for _, h := range builtinHints {
		if h.pattern.MatchString(message) {
			return h.text
		}
	}
	return ""
}
