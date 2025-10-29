package tracing

import (
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// BusinessCorrelation holds business identifiers for cross-service correlation
type BusinessCorrelation struct {
	Queue       string   `json:"queue"`
	JobSet      string   `json:"jobset"`
	JobIDs      []string `json:"job_ids,omitempty"`
	Operation   string   `json:"operation"` // "submit", "cancel", "preempt"
}

// AddBusinessCorrelation adds business correlation attributes to a span
func AddBusinessCorrelation(span trace.Span, corr BusinessCorrelation) {
	span.SetAttributes(
		attribute.String("armada.queue", corr.Queue),
		attribute.String("armada.jobset", corr.JobSet),
		attribute.String("armada.operation", corr.Operation),
	)

	if len(corr.JobIDs) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.job_ids", corr.JobIDs),
			attribute.Int("armada.job_count", len(corr.JobIDs)),
		)
	}
}

// BatchCorrelation holds business identifiers extracted from a batch of operations
type BatchCorrelation struct {
	Queues    []string
	JobSets   []string
	JobIDs    []string
	Operations []string
}

// AddBatchCorrelation adds batch-level business correlation attributes to a span
func AddBatchCorrelation(span trace.Span, corr BatchCorrelation) {
	if len(corr.Queues) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.queues", corr.Queues),
			attribute.Int("armada.batch.unique_queues", len(corr.Queues)),
		)
	}

	if len(corr.JobSets) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.jobsets", corr.JobSets),
			attribute.Int("armada.batch.unique_jobsets", len(corr.JobSets)),
		)
	}

	if len(corr.JobIDs) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.job_ids", corr.JobIDs),
			attribute.Int("armada.batch.total_jobs", len(corr.JobIDs)),
		)
	}

	if len(corr.Operations) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.operations", corr.Operations),
		)
	}
}

// CreateSubmissionOperationID creates a unique operation ID for job submissions
func CreateSubmissionOperationID(queue, jobset string) string {
	return fmt.Sprintf("submit-%s-%s-%d", queue, jobset, time.Now().Unix())
}

// CreateCancellationOperationID creates a unique operation ID for job cancellations
func CreateCancellationOperationID(queue, jobset string) string {
	return fmt.Sprintf("cancel-%s-%s-%d", queue, jobset, time.Now().Unix())
}