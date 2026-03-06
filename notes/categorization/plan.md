# Error Categorization - Implementation Plan

## Problem

Armada has no consistent way to classify **why** a job failed. Raw K8s signals (exit codes, termination messages, pod conditions) flow through the system but carry no semantic meaning. This blocks:

1. **Observability** - users can't filter/search by failure type in Lookout
2. **Retry policies (#4683)** - the `onFailureCategory` rule type needs categories to exist first

## Architecture Decision

**Categories are assigned at the executor, not the scheduler.**

Rationale:
- Executor has the richest data: full pod status, container states
- Different clusters need different categories (GPU vs CPU workloads)
- Categories travel as strings through Pulsar/DB, so the scheduler doesn't need to understand the matching rules
- Keeps the scheduler simple - it just reads category labels

## Data Flow

```
Pod fails on K8s cluster
  -> Executor detects failure (pod_issue_handler.go / job_state_reporter.go)
  -> categorizer.Classify(pod) -> []string{"cuda_error"}
  -> ExtractFailureInfo(pod, retryable, msg, categories) -> FailureInfo proto
  -> FailureInfo attached to Error proto
  -> Event published to Pulsar
  -> Ingester stores Error blob (compressed) in job_run_errors - no new DB columns
  -> Scheduler reads FailureInfo for retry decisions
  -> Lookout reads FailureInfo for display
```

## Package Structure

Two packages, designed so both executor (categorization) and scheduler (retry #4683) can reuse matching logic:

- `internal/common/errormatch` - shared matching primitives (`ExitCodeMatcher`, `RegexMatcher`, `MatchExitCode()`, `MatchPattern()`, condition constants)
- `internal/executor/categorizer` - configurable classification engine (`Classifier`, `CategoryConfig`, `CategoryRule`)

## Configuration

```yaml
kubernetes:
  errorCategories:
    - name: oom
      rules:
        - onConditions: ["OOMKilled"]
    - name: cuda_error
      rules:
        - onExitCodes:
            operator: In
            values: [74, 75]
        - onTerminationMessage:
            pattern: "(?i)cuda.*error"
    - name: transient_infra
      rules:
        - onConditions: ["Evicted"]
        - onExitCodes:
            operator: In
            values: [137]
```

Rules within a category are OR'd. Each rule specifies exactly one matcher type: `onConditions`, `onExitCodes`, or `onTerminationMessage`. Validated at startup.

---

## PR Sequence

### PR 1: Proto schema + shared types + executor classifier (DONE)

**PR:** https://github.com/armadaproject/armada/pull/4741

Proto changes:
- `FailureCondition` enum (Unspecified, Preempted, Evicted, OOMKilled, DeadlineExceeded, Unschedulable, UserError)
- `FailureInfo` message (exit_code, condition, pod_check_retryable, pod_check_message, termination_message, categories)
- `failure_info` field on `Error` proto (field 15, outside oneof)

Shared matching (`internal/common/errormatch`):
- `ExitCodeMatcher` (In/NotIn operators), `RegexMatcher`, condition constants
- `MatchExitCode()`, `MatchPattern()` functions

Executor classifier (`internal/executor/categorizer`):
- `NewClassifier()` validates config upfront (unknown conditions, invalid operators, bad regexes, exactly one matcher per rule)
- `Classify(pod)` returns matching category names
- Checks all containers (regular + init), exit code 0 skipped

Executor utilities:
- `ExtractFailureInfo()` in `pod_status.go` - builds `FailureInfo` proto from pod status
- `isPreempted()` - detects preemption via DisruptionTarget condition
- `mapReasonToCondition()` - maps KubernetesReason to FailureCondition enum

Config: `ErrorCategories` field on `KubernetesConfiguration`

**Status: Complete.**

---

### PR 2: Wire categories into executor event reporting

**Goal:** Attach `FailureInfo` (with categories) to error events sent to Pulsar.

**Files:**
- `internal/executor/reporter/event.go` - `CreateJobFailedEvent()` and `CreateReturnLeaseEvent()` accept `*armadaevents.FailureInfo`
- `internal/executor/service/pod_issue_handler.go` - Inject `Classifier`, call `Classify()` + `ExtractFailureInfo()` when reporting failures
- `internal/executor/service/job_state_reporter.go` - Same: inject classifier, attach categories
- `internal/executor/executorapp.go` - Construct `Classifier` from config at startup

**Feature flag:** If `ErrorCategories` config is empty, classifier returns nil categories. `FailureInfo` is still attached (with condition, exit code, etc.) but `categories` is empty.

**Risk:** Medium. Changes event reporting path, but additive only (new field on existing proto).

---

### PR 3: Scheduler reads FailureInfo

**Goal:** Ensure the scheduler can read `FailureInfo` from run errors for retry decisions.

No schema changes needed - the ingester already stores the full `Error` proto as a compressed blob in `job_run_errors` (`instructions.go:276`). Adding `FailureInfo` to `Error` means it's already persisted.

**Files:**
- `internal/scheduler/scheduler.go` - Add `extractFailureInfo()` helper
- Round-trip test: FailureInfo with categories survives marshal/compress/decompress/unmarshal

**Risk:** Low. Read-only access to data already stored.

---

### PR 4: Lookout displays categories

**Goal:** Show error categories in Lookout when viewing failed job run details.

Can proceed in parallel with PR 3 since it's a separate read path.

**Risk:** Low. Read-only display.

---

### PR 5: Retry engine `onFailureCategory` rule type (#4683)

**Goal:** Add `onFailureCategory` matcher to the retry engine.

**Files:**
- `internal/scheduler/configuration/retry.go` - Add `OnFailureCategory []string` to `Rule` (imports `errormatch` types)
- `internal/scheduler/retry/matcher.go` - Add `matchesCategory()`. Rule evaluation: conditions > exit codes > termination message > failure category

**Risk:** Low. Additive to the retry engine.

---

## Dependency Graph

```
PR 1 (proto + classifier) [DONE]
  -> PR 2 (wire events)
       -> PR 3 (scheduler reads) -> PR 5 (retry onFailureCategory)
       -> PR 4 (Lookout, parallel with PR 3)
```

## Summary

| PR | Description | Status | Risk |
|----|-------------|--------|------|
| 1 | Proto + shared types + executor classifier | **Done** (#4741) | Low |
| 2 | Wire classifier into executor event reporting | **Done** | Med |
| 3 | Scheduler reads FailureInfo from errors | Planned | Low |
| 4 | Lookout displays categories (parallel) | Planned | Low |
| 5 | Retry engine onFailureCategory (#4683) | Planned | Low |

## Follow-up PRs

After the core 5-PR sequence lands, these are additive extensions. Grouped by type of change.

### New matcher types on `CategoryRule`

Same pattern as existing matchers: add a field to `CategoryRule`, a validation check in `buildRule()`, a case in `ruleMatches()`.

| Matcher | What it matches | Where the data comes from | Effort |
|---------|----------------|--------------------------|--------|
| `onPodEvents` | K8s warning events (FailedScheduling, FailedMount) | `client.CoreV1().Events()` - need to fetch events for the pod | Medium |
| `onNodeConditions` | DiskPressure, MemoryPressure, NodeNotReady | `pod.Spec.NodeName` -> node status. Executor already watches nodes | Medium |
| `onContainerType` | Init vs regular vs sidecar | `pod.Status.InitContainerStatuses` vs `ContainerStatuses` | Low |

### New fields on `FailureInfo` proto

Add a proto field, populate in `ExtractFailureInfo()`. No matcher changes needed.

| Field | Purpose | Effort |
|-------|---------|--------|
| `bool user_code_ran` | Did any non-init container start? Distinguishes infra failures from user code failures | Low - check if any container has `StartedAt` set |
| `string container_name` | Name of first failed container | Low - already available, just not on FailureInfo. Already on `ContainerError.ObjectMeta.Name` |

### Require new data sources (bigger scope, separate design)

| Idea | Data source | Challenge |
|------|-------------|-----------|
| XID errors (NVIDIA GPU) | Kernel logs / dmesg on the node | Need node-level log scraping or device plugin integration |
| Stack trace parsing | Container logs via K8s API | Cost at scale (5-10M pods/day), log retrieval latency |
| Last N error log lines | Container logs via K8s API | Same cost concern. Need to decide where to store (proto blob grows) |
| Pod status snapshot | Full `pod.Status` | Already partially covered by the Error proto blob. Full snapshot significantly larger |

## Design Decisions

1. **Blob-only storage (no indexed columns):** FailureInfo lives inside the compressed Error proto blob. No new DB columns for v1. Add indexed columns when Lookout needs fast category filtering.

2. **One matcher per rule:** Each rule specifies exactly one of onConditions/onExitCodes/onTerminationMessage. Validated at construction. Rules within a category are OR'd.

3. **Shared errormatch package:** Matching types and functions in `internal/common/errormatch` so both executor categorizer and scheduler retry engine (#4683) can import them without duplication.
