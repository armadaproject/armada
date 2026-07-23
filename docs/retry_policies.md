# Retry policies
- [Retry policies](#retry-policies)
  - [Overview](#overview)
  - [Enabling the retry engine](#enabling-the-retry-engine)
  - [Policy format](#policy-format)
    - [Matching semantics](#matching-semantics)
    - [Match fields](#match-fields)
  - [Retry budgets](#retry-budgets)
  - [Gang jobs](#gang-jobs)
  - [Pod naming](#pod-naming)
  - [Per-job opt-out](#per-job-opt-out)
  - [Managing policies with armadactl](#managing-policies-with-armadactl)
  - [Rollout guide for operators](#rollout-guide-for-operators)

## Overview

Retry policies let operators define, per queue, which job failures Armada should retry and which it should fail permanently. A retry policy is a named resource, managed through `armadactl` like a queue, and attached to one or more queues by name. When a job run fails, the scheduler looks up the policy attached to the job's queue, evaluates the policy rules against the failure, and either requeues the job for another attempt or fails it terminally.

The retry engine is off by default. It only runs when `scheduling.retryPolicy.enabled` is set to `true` in the scheduler configuration. With the flag off, or for queues with no policy attached, Armada behaves exactly as before: jobs are only re-leased on lease returns, up to the legacy attempt limit.

## Enabling the retry engine

The engine is controlled by the `retryPolicy` block under the `scheduling` section of the scheduler configuration:

```yaml
scheduling:
  retryPolicy:
    enabled: true
    globalMaxRetries: 5
    defaultPolicyName: fleet-default   # optional
```

* `enabled`: turns the engine on. Defaults to `false`.
* `globalMaxRetries`: a scheduler-wide cap on retries per job. See [Retry budgets](#retry-budgets) for the exact semantics. With `enabled: true` and `globalMaxRetries: 0` the engine runs but never grants a retry (kill-switch semantics), and the scheduler logs a warning at startup.
* `defaultPolicyName`: optional. The retry policy applied to jobs whose queue has no policy of its own, letting you turn retries on fleet-wide with one named policy. When empty, only queues with an explicitly attached policy get engine decisions and every other queue keeps legacy behaviour.

Before enabling the flag, read the [rollout guide](#rollout-guide-for-operators). In particular, all executors must be upgraded before the flag is enabled anywhere.

## Policy format

Policies are written as YAML (or JSON) files and created with `armadactl`. A realistic example:

```yaml
apiVersion: armadaproject.io/v1beta1
kind: RetryPolicy
name: ml-training-retries
retryLimit: 3
defaultAction: Fail
rules:
  # Known-fatal user errors: fail immediately, never retry.
  - action: Fail
    onCategory: user-error
  # Transient GPU faults are worth another attempt.
  - action: Retry
    onCategory: gpu
    onSubcategory: transient
  # Infrastructure failures categorised by Armada.
  - action: Retry
    onCategory: internal
    onSubcategory: lease-expired
```

* `name`: unique name of the policy. Queues reference policies by this name.
* `retryLimit`: maximum number of retries after the initial failure. See [Retry budgets](#retry-budgets).
* `defaultAction`: `Retry` or `Fail`. Applied when no rule matches.
* `rules`: an ordered list of matching rules, each with an `action` (`Retry` or `Fail`) and one or more match fields.

### Matching semantics

* Rules match on the failure category the executor's categorizer assigned to the error. Rules are evaluated top to bottom and the first matching rule wins. Later rules are not consulted.
* If no rule matches, `defaultAction` decides.

Order rules from most specific to most general. A common pattern is to put `Fail` rules for known-fatal categories first, followed by `Retry` rules for transient ones, with `defaultAction: Fail` as the safety net.

### Match fields

* `onCategory`: matches the failure category Armada assigned to the error, for example `internal`, `gpu`, or `user-error`. Categories are defined by the executor's error categorizer.
* `onSubcategory`: narrows a category match to a specific subcategory, for example `internal` / `lease-expired`. Only valid together with `onCategory`.

Category and subcategory matching is exact and case-sensitive, so the values here must match what the executor's categorizer emits byte for byte.

Matching on failure signals directly (exit codes, Kubernetes conditions, termination-message patterns) is planned for a later version. For now, express those by defining a category for them in the executor's categorizer config and matching the category here.

## Retry budgets

Two limits bound how often a job is retried: the per-policy `retryLimit` and the scheduler-wide `globalMaxRetries`.

* `retryLimit` counts retries, not attempts. `retryLimit: 3` allows 3 retries after the initial failure, so 4 total attempts before the job fails terminally. `retryLimit: 0` allows no retries, so a job under that policy fails terminally on its first failure.
* Preemptions never consume the failure budget. A preempted run does not count against `retryLimit` or `globalMaxRetries`, so a job that was preempted earlier can still use its full failure budget when it later fails on its own. This version does not retry preempted runs themselves: a preemption is not treated as a job failure, and preemption-driven retries are planned for a later version.
* `globalMaxRetries` is a scheduler-wide cap that applies on top of every policy. Like `retryLimit`, it counts genuine-failure retries only: preempted and lease-returned runs never consume it. It bounds the same value each policy's `retryLimit` bounds, just with a single scheduler-wide ceiling, so no policy can grant more retries than the cap allows. Legacy lease returns are bounded separately by the legacy attempt limit, not by this cap.
* `globalMaxRetries: 0` disables all engine retries. This is the kill switch: with the cap at zero the engine never grants a retry, whatever the policies say.
* There is no unlimited setting for the global cap. Every deployment with the engine enabled has a finite scheduler-wide bound on retries per job.

## Gang jobs

Gang jobs are excluded from the retry engine in this version. Retrying a gang atomically requires aggregating failures across all members and restarting them together, which is not yet implemented. A job that is part of a gang with cardinality 2 or more is never retried by the engine, even if a policy rule matches. When that happens, the scheduler increments the `armada_scheduler_retry_policy_gang_skipped_total` metric and logs at info level, so you can see how often policies would have applied to gangs.

Gangs with cardinality 1 are treated as plain jobs and do retry.

Gang retry support is tracked in [armadaproject/armada#4683](https://github.com/armadaproject/armada/issues/4683).

## Pod naming and collision avoidance

The `action: Delete` pod-deletion behaviour described in this section ships with the executor failed-pod-deletion change. Until that is deployed, the executor does not delete failed pods on categorisation, so the collision this section describes can occur on retry.

Every attempt of a job reuses the same pod name, `armada-<jobId>-0`. A retry can therefore collide with the failed pod of the previous attempt if that pod is still terminating on the same cluster. To avoid this, the executor deletes a failed pod as soon as its failure is classified into a category configured with `action: Delete`, which frees the name before the retry is leased.

This has an operational consequence: **every failure category that a retry rule matches on must be configured with `action: Delete` on the executor.** If a retried category is left as the default `action: Retain`, the retained pod causes the retry's lease to fail with an `AlreadyExists` error. That surfaces as a recoverable submit error, so the run's lease is returned to the scheduler. A returned lease is not a categorized pod failure, so the retry engine does not decide it and it falls through to the legacy attempt-limit path. Once the legacy attempt limit is hit the job fails terminally with a `MaxRunsExceeded` reason that does not mention the collision, so the real cause is easy to miss. Collision handling also deletes the retained pod, so the debugging evidence that `Retain` was meant to preserve is gone anyway.

## Per-job opt-out

Jobs submitted with `failFast: true` (the `armadaproject.io/failFast` annotation) bypass the retry engine entirely. A fail-fast job fails terminally on its first failure regardless of the queue's retry policy. Use this for workloads where a repeated attempt is wasted work, for example jobs that are resubmitted by an external workflow engine with its own retry logic.

## Managing policies with armadactl

Create a policy from a file:

```bash
armadactl create retry-policy -f retry-policy.yaml
```

Update an existing policy in place. The change takes effect for failures evaluated after the scheduler's policy cache refreshes:

```bash
armadactl update retry-policy -f retry-policy.yaml
```

Inspect policies:

```bash
armadactl get retry-policy ml-training-retries
armadactl get retry-policies
```

Attach a policy to a queue at creation time, or to an existing queue:

```bash
armadactl create queue my-queue --retry-policy ml-training-retries
armadactl update queue my-queue --retry-policy ml-training-retries
```

Delete a policy:

```bash
armadactl delete retry-policy ml-training-retries
```

Deletion is rejected while any queue still references the policy. Detach it from all queues first, then delete it.

Managing policies requires the `create_retry_policy`, `update_retry_policy`, and `delete_retry_policy` permissions. Grant them through the server's permission group mapping; without them the corresponding CRUD calls return `PermissionDenied`.

## Rollout guide for operators

**Configure `action: Delete` on retried categories before enabling the flag.** A retry reuses the failed attempt's pod name, so the executor must delete the failed pod to free the name (see [Pod naming and collision avoidance](#pod-naming-and-collision-avoidance)). Any failure category a retry rule matches on must set `action: Delete` in the executor's categorizer config. If it does not, the retry collides with the retained pod, the lease is returned as a recoverable submit error, and the job eventually fails terminally with a misleading `MaxRunsExceeded` reason (see [Pod naming and collision avoidance](#pod-naming-and-collision-avoidance) for the full failure mode). Audit the categorizer config against your retry rules before turning the flag on.

**Disabling the flag mid-flight is safe.** Jobs that were already retried keep running. New failures fall back to legacy behaviour: the legacy attempt limit applies instead of policy budgets. No job state is lost.

**Metrics to alert on:**

* Policy cache refresh failures and cache staleness. The scheduler periodically refreshes policies from the API. The cache has no expiry: on a refresh failure it fails open and keeps serving the last good policies indefinitely, so retries continue through a short API outage. Alert on the refresh-failure counter and the staleness gauge anyway, because a policy edited during a prolonged outage does not take effect until the API recovers.
* Invalid-policy skips. A policy that fails validation (for example an unknown `action`, or a rule with no `onCategory`) is skipped at cache refresh and the queues referencing it fall back to legacy behaviour.
* Gang skips (`armada_scheduler_retry_policy_gang_skipped_total`). A steadily growing count means users are attaching retry policies to queues that run gangs and expecting retries that never happen.
* Retry decision counters. The `armada_scheduler_retry_policy_decisions_total` counter is labelled by policy and decision; the queue is in the scheduler log, not the metric. Track retry and fail rates per policy to spot policies that retry far more (or less) than intended, and to catch jobs burning through budgets on hopeless failures.
