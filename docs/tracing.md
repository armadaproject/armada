# Tracing job submissions

This guide is for operators configuring submitter context propagation and server submission spans. For an annotation example, see [Submitter trace context](creating_and_submitting_jobs.md#submitter-trace-context).

## Server configuration

Set `submission.traceContext.enabled: true` in the server configuration to store submitter context with new jobs. Propagation works independently of span export. These are the defaults:

```yaml
submission:
  traceContext:
    enabled: false
    propagateGrpcBaggage: false
    aliasAnnotationPrefixes: []  # For example: [otel.example.com/]
    maxTraceStateBytes: 512
    maxBaggageBytes: 8192
```

Also enable `observability.enabled` and configure its exporter to emit submission spans.

## Context selection

The server tries these sources in order and uses the first valid context:

| Priority | Source | Result |
| --- | --- | --- |
| 1 | `otel.armadaproject.io/{traceparent,tracestate,baggage}` item annotations | Keep the supplied context. |
| 2 | The same suffixes under each configured alias prefix, in order | Copy to the canonical `otel.armadaproject.io/` keys; retain aliases. |
| 3 | Incoming gRPC metadata with a valid W3C `traceparent` | Copy to canonical keys, subject to the request-baggage opt-in. |
| None | No valid source | Add no context and emit no job root. |

Traceparent, tracestate and baggage always come from one source. A missing field in a selected fallback clears any stale canonical value. Invalid traceparents, oversized tracestate or oversized forwarded baggage cause the server to try the next source. Zero byte limits use the defaults above. Duplicate submissions keep their original context.

## Baggage controls and limits

- Incoming gRPC baggage is omitted unless `propagateGrpcBaggage: true` is set. Traceparent and tracestate still propagate with the flag off, even if the ignored baggage is oversized.
- Baggage explicitly supplied in canonical or configured alias annotations is unaffected by that flag. All forwarded baggage must fit within `maxBaggageBytes`.
- Audit baggage added by gateways or service meshes before enabling request baggage. It is stored in job annotations and may reach job containers; do not include secrets.
- The byte limits do not filter sensitive contents or cap the total size of existing annotations. Copying context adds to that metadata budget.
- With submission tracing enabled, canonical trace fields are literal: `{JobId}` in baggage is not expanded.

## Submission spans

When propagation and server observability are both enabled, each accepted new job with sampled context emits an `armada.job.submit` root after successful publication. It covers request handling and includes `armada.queue`, `armada.job_set.id` and `armada.job.id`.

| Identifier | Value |
| --- | --- |
| Trace ID | The job ULID's 16 bytes, represented as hexadecimal. |
| Root span ID | The job ULID's last eight bytes. |

The root has one link to the selected submitter span, attributed with `armada.trace_context.source`. For gRPC context with server tracing enabled, the target is the server span in that incoming trace. The link keeps the full tracestate; the root keeps only `ot`. Sampling follows the selected context. Duplicates, failed submissions and jobs without sampled context emit no job roots.

### Sampling and export

ULID-derived IDs are correlated within a batch. Avoid downstream ratio or probabilistic sampling that assumes independent, uniformly random trace IDs; honor the propagated decision or use a policy that supports ULID-derived IDs.

Export failures do not fail submission. A full export queue can drop spans, so absence of a span does not imply submission failure. A job ID with an all-zero span-ID suffix cannot form a valid W3C span and is skipped with a warning.

## Metrics

`armada_trace_context_source_total{source="annotation|alias|grpc|none|invalid"}` records context resolution for new jobs while the feature is enabled:

- Each new job increments its selected source or `none`.
- `invalid` additionally counts rejected sources before fallback; summing all labels is not a job count.
- Duplicate jobs are excluded.
