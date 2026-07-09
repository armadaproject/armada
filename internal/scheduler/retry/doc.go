// Package retry decides whether a failed job run should be retried or failed
// permanently.
//
// The decision is driven by named retry policies, authored as api.RetryPolicy
// protos and attached to queues by name. A policy holds an ordered list of
// rules, a default action, and a per-policy retry limit. Each rule matches on
// the failure category the executor assigned to the run error, and optionally
// on a subcategory that narrows the match. The first rule that matches wins.
// If no rule matches, the policy's default action applies.
//
// Engine.Evaluate makes the decision. It is a pure function of the compiled
// policy, the run error, and the job's failure counts; its doc comment
// defines the check order and the exact limit semantics.
//
// ConvertPolicy compiles api.RetryPolicy protos into the Policy type the
// engine evaluates. ApiPolicyCache keeps the compiled policies in memory,
// refreshed from the Armada API, and NoopPolicyCache stands in when the
// retry policy feature is disabled.
package retry
