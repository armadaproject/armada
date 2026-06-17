package observability

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/armadaproject/armada/internal/common/logging"
)

const (
	// DefaultAttributeCardinalityLimit is the maximum number of unique values
	// allowed per custom span attribute key before values are redacted.
	DefaultAttributeCardinalityLimit = 1000

	// AttributeRedactedValue is used when an attribute key is denied for PII/sensitive content.
	AttributeRedactedValue = "[REDACTED]"
	// AttributeDisallowedValue is used when an attribute key is not in the allow-list.
	AttributeDisallowedValue = "[DISALLOWED]"
	// AttributeHighCardinalityValue is used once cardinality guardrails are exceeded.
	AttributeHighCardinalityValue = "[HIGH_CARDINALITY]"
)

// SpanAttributePolicy defines guardrails for span attribute keys and values.
//
// Policy summary:
//   - Allow-list: rpc.*, http.*, net.*, server.*, service.*, armada.*, trace_id, span_id
//   - Deny-list: explicit sensitive keys and key-name patterns (password/secret/token/key)
//   - Cardinality: custom (non-standard) keys capped at DefaultAttributeCardinalityLimit unique values
//
// Important: OTel SDK span processors cannot delete already-set attributes from an active span.
// To prevent raw PII leakage, denied/disallowed keys are overwritten with marker values.
type SpanAttributePolicy struct {
	allowedPrefixes []string
	allowedExact    map[string]struct{}
	deniedExact     map[string]struct{}
	deniedContains  []string

	cardinalityExemptPrefixes []string
	cardinalityExemptExact    map[string]struct{}
	cardinality               *attributeCardinalityTracker
}

type SpanAttributeViolationReason string

const (
	SpanAttributeViolationDenied          SpanAttributeViolationReason = "denied"
	SpanAttributeViolationDisallowed      SpanAttributeViolationReason = "disallowed"
	SpanAttributeViolationHighCardinality SpanAttributeViolationReason = "high_cardinality"
)

type SpanAttributeViolation struct {
	Key    string
	Reason SpanAttributeViolationReason
}

// NewDefaultSpanAttributePolicy returns the default attribute policy for Armada traces.
func NewDefaultSpanAttributePolicy() *SpanAttributePolicy {
	return &SpanAttributePolicy{
		allowedPrefixes: []string{"rpc.", "http.", "net.", "server.", "service.", "armada."},
		allowedExact: map[string]struct{}{
			"trace_id": {},
			"span_id":  {},
		},
		// Explicit deny-list for common PII/sensitive payload fields.
		deniedExact: map[string]struct{}{
			"user_id":       {},
			"user_email":    {},
			"user_name":     {},
			"password":      {},
			"api_key":       {},
			"token":         {},
			"secret":        {},
			"job_payload":   {},
			"request_body":  {},
			"response_body": {},
		},
		deniedContains:            []string{"password", "secret", "token", "api_key", "apikey", "key"},
		cardinalityExemptPrefixes: []string{"rpc.", "http.", "net.", "server.", "service."},
		cardinalityExemptExact: map[string]struct{}{
			"trace_id":        {},
			"span_id":         {},
			"http.user_agent": {},
		},
		cardinality: newAttributeCardinalityTracker(DefaultAttributeCardinalityLimit),
	}
}

// SanitizeForSpan returns attributes safe for emission by applying deny-list,
// allow-list, and cardinality guardrails.
//
// This function only sanitizes the attributes passed to it. In the OTel
// SDK processor model, OnStart can sanitize initial attributes, but attributes added
// later via span.SetAttributes(...) cannot be rewritten at OnEnd. See
// ViolationsForSpan/OnEnd guardrails for post-start detection.
func (p *SpanAttributePolicy) SanitizeForSpan(attrs []attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, kv := range attrs {
		key := string(kv.Key)
		keyLower := strings.ToLower(key)

		switch {
		case p.isDenied(keyLower):
			out = append(out, attribute.String(key, AttributeRedactedValue))
			continue
		case !p.isAllowed(keyLower):
			out = append(out, attribute.String(key, AttributeDisallowedValue))
			continue
		}

		if p.shouldGuardCardinality(keyLower) && p.cardinality.ExceedsLimit(keyLower, attributeValueFingerprint(kv.Value)) {
			out = append(out, attribute.String(key, AttributeHighCardinalityValue))
			continue
		}

		out = append(out, kv)
	}
	return out
}

// ViolationsForSpan returns policy violations present in the provided attributes.
//
// Use this as a guardrail for ended spans where mutation is no longer possible,
// e.g. to detect attributes that were added after OnStart and therefore bypassed
// processor-time sanitization.
func (p *SpanAttributePolicy) ViolationsForSpan(attrs []attribute.KeyValue) []SpanAttributeViolation {
	violations := make([]SpanAttributeViolation, 0)
	for _, kv := range attrs {
		if isSanitizedMarkerValue(kv.Value) {
			continue
		}

		key := string(kv.Key)
		keyLower := strings.ToLower(key)

		switch {
		case p.isDenied(keyLower):
			violations = append(violations, SpanAttributeViolation{Key: key, Reason: SpanAttributeViolationDenied})
		case !p.isAllowed(keyLower):
			violations = append(violations, SpanAttributeViolation{Key: key, Reason: SpanAttributeViolationDisallowed})
		case p.shouldGuardCardinality(keyLower) && p.cardinality.IsOverLimit(keyLower, attributeValueFingerprint(kv.Value)):
			violations = append(violations, SpanAttributeViolation{Key: key, Reason: SpanAttributeViolationHighCardinality})
		}
	}
	return violations
}

func isSanitizedMarkerValue(value attribute.Value) bool {
	if value.Type() != attribute.STRING {
		return false
	}

	s := value.AsString()
	return s == AttributeRedactedValue || s == AttributeDisallowedValue || s == AttributeHighCardinalityValue
}

func (p *SpanAttributePolicy) isAllowed(key string) bool {
	if _, ok := p.allowedExact[key]; ok {
		return true
	}
	for _, prefix := range p.allowedPrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func (p *SpanAttributePolicy) isDenied(key string) bool {
	if _, ok := p.deniedExact[key]; ok {
		return true
	}
	for _, disallowedPart := range p.deniedContains {
		if strings.Contains(key, disallowedPart) {
			return true
		}
	}
	return false
}

func (p *SpanAttributePolicy) shouldGuardCardinality(key string) bool {
	if _, ok := p.cardinalityExemptExact[key]; ok {
		return false
	}
	for _, prefix := range p.cardinalityExemptPrefixes {
		if strings.HasPrefix(key, prefix) {
			return false
		}
	}
	return true
}

func attributeValueFingerprint(value attribute.Value) string {
	return fmt.Sprintf("%d:%v", value.Type(), value.AsInterface())
}

type attributeCardinalityTracker struct {
	limit int

	mu        sync.Mutex
	seen      map[string]map[string]struct{}
	overLimit map[string]bool
}

func newAttributeCardinalityTracker(limit int) *attributeCardinalityTracker {
	return &attributeCardinalityTracker{
		limit:     limit,
		seen:      make(map[string]map[string]struct{}),
		overLimit: make(map[string]bool),
	}
}

func (c *attributeCardinalityTracker) ExceedsLimit(key, value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	knownValues, ok := c.seen[key]
	if !ok {
		knownValues = make(map[string]struct{})
		c.seen[key] = knownValues
	}

	if _, seen := knownValues[value]; seen {
		return false
	}

	if len(knownValues) >= c.limit {
		c.overLimit[key] = true
		return true
	}

	knownValues[value] = struct{}{}
	return false
}

func (c *attributeCardinalityTracker) IsOverLimit(key, value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.overLimit[key] {
		return true
	}

	knownValues, ok := c.seen[key]
	if !ok {
		return false
	}

	_, seen := knownValues[value]
	return !seen && len(knownValues) >= c.limit
}

type spanAttributePolicyProcessor struct {
	policy *SpanAttributePolicy
}

// NewSpanAttributePolicyProcessor creates a span processor that sanitizes span attributes
// at span start using the configured policy and applies OnEnd guardrails for attributes
// added after span start.
//
// Constraint: OTel SDK processors cannot mutate attributes on ended spans. Therefore,
// attributes set via span.SetAttributes(...) after OnStart cannot be rewritten at
// processor level. OnEnd logs policy violations as an operational guardrail.
func NewSpanAttributePolicyProcessor(policy *SpanAttributePolicy) sdktrace.SpanProcessor {
	if policy == nil {
		policy = NewDefaultSpanAttributePolicy()
	}
	return &spanAttributePolicyProcessor{policy: policy}
}

func (p *spanAttributePolicyProcessor) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	attrs := span.Attributes()
	if len(attrs) == 0 {
		return
	}
	sanitized := p.policy.SanitizeForSpan(attrs)
	span.SetAttributes(sanitized...)
}

func (p *spanAttributePolicyProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	violations := p.policy.ViolationsForSpan(span.Attributes())
	if len(violations) == 0 {
		return
	}

	keys := make([]string, 0, len(violations))
	for _, violation := range violations {
		keys = append(keys, fmt.Sprintf("%s(%s)", violation.Key, violation.Reason))
	}

	logging.Warnf(
		"Span %q contains policy-violating attributes that could not be rewritten after span start: %s",
		span.Name(),
		strings.Join(keys, ", "),
	)
}

func (p *spanAttributePolicyProcessor) Shutdown(context.Context) error { return nil }

func (p *spanAttributePolicyProcessor) ForceFlush(context.Context) error { return nil }
