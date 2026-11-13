package observability

import (
	"context"
	"encoding/binary"
	"math/rand/v2"
	"time"

	"github.com/oklog/ulid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/armadaproject/armada/internal/common/logging"
)

type jobSpanKey struct{}

// RecordJobSubmission emits an accepted job's root using the existing provider.
// Call only after publication succeeds. A disabled provider remains a no-op.
func RecordJobSubmission(jobID, queue, jobSet, source string, submitter trace.SpanContext, start, end time.Time) {
	if !submitter.IsValid() || !submitter.IsSampled() {
		return
	}
	id, err := ulid.Parse(jobID)
	if err != nil {
		logging.WithError(err).Warn("Cannot trace job with invalid ULID")
		return
	}
	state := trace.TraceState{}
	if value := submitter.TraceState().Get("ot"); value != "" {
		state, _ = state.Insert("ot", value)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID(id), SpanID: trace.SpanID(id[8:]), TraceFlags: trace.FlagsSampled, TraceState: state,
	})
	// W3C forbids zero IDs; skip the exceedingly rare zero ULID suffix.
	if !sc.IsValid() {
		logging.Warn("Cannot trace job with zero trace or span ID")
		return
	}
	ctx := context.WithValue(context.Background(), jobSpanKey{}, sc)
	_, span := otel.Tracer("armada/job-submission").Start(ctx, "armada.job.submit",
		trace.WithNewRoot(), trace.WithTimestamp(start),
		trace.WithAttributes(attribute.String("armada.queue", queue), attribute.String("armada.job_set.id", jobSet), attribute.String("armada.job.id", jobID)),
		trace.WithLinks(trace.Link{SpanContext: submitter, Attributes: []attribute.KeyValue{attribute.String("armada.trace_context.source", source)}}),
	)
	span.End(trace.WithTimestamp(end))
}

// JobSpanIDGenerator preserves deterministic job root IDs, with random IDs for
// ordinary spans. Its context key is private to RecordJobSubmission.
type JobSpanIDGenerator struct{}

func (JobSpanIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if sc, ok := ctx.Value(jobSpanKey{}).(trace.SpanContext); ok {
		return sc.TraceID(), sc.SpanID()
	}
	var id trace.TraceID
	for !id.IsValid() {
		binary.NativeEndian.PutUint64(id[:8], rand.Uint64())
		binary.NativeEndian.PutUint64(id[8:], rand.Uint64())
	}
	return id, (JobSpanIDGenerator{}).NewSpanID(ctx, id)
}

func (JobSpanIDGenerator) NewSpanID(_ context.Context, _ trace.TraceID) trace.SpanID {
	var id trace.SpanID
	for !id.IsValid() {
		binary.NativeEndian.PutUint64(id[:], rand.Uint64())
	}
	return id
}

// JobSpanSampler follows resolved sampling for job roots and delegates all other
// spans to the configured sampler, without making the submitter a parent.
type JobSpanSampler struct{ Fallback sdktrace.Sampler }

func (s JobSpanSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if sc, ok := p.ParentContext.Value(jobSpanKey{}).(trace.SpanContext); ok {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: sc.TraceState()}
	}
	return s.Fallback.ShouldSample(p)
}

func (s JobSpanSampler) Description() string {
	return "JobSpanSampler(" + s.Fallback.Description() + ")"
}
