package tracing

import (
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// JobMetadata holds job identifiers for cross-service correlation
type JobMetadata struct {
	Queue     string   `json:"queue"`
	JobSet    string   `json:"jobset"`
	JobIDs    []string `json:"job_ids,omitempty"`
	Operation string   `json:"operation"` // "submit", "cancel", "preempt"
}

// AddJobMetadata adds job metadata attributes to a span
func AddJobMetadata(span trace.Span, metadata JobMetadata) {
	span.SetAttributes(
		attribute.String("armada.queue", metadata.Queue),
		attribute.String("armada.jobset", metadata.JobSet),
		attribute.String("armada.operation", metadata.Operation),
	)

	if len(metadata.JobIDs) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.job_ids", metadata.JobIDs),
			attribute.Int("armada.job_count", len(metadata.JobIDs)),
		)
	}
}

// BatchMetadata holds job identifiers extracted from a batch of operations
type BatchMetadata struct {
	Queues     []string
	JobSets    []string
	JobIDs     []string
	Operations []string
}

// AddBatchMetadata adds batch-level job metadata attributes to a span
func AddBatchMetadata(span trace.Span, metadata BatchMetadata) {
	if len(metadata.Queues) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.queues", metadata.Queues),
			attribute.Int("armada.batch.unique_queues", len(metadata.Queues)),
		)
	}

	if len(metadata.JobSets) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.jobsets", metadata.JobSets),
			attribute.Int("armada.batch.unique_jobsets", len(metadata.JobSets)),
		)
	}

	if len(metadata.JobIDs) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.job_ids", metadata.JobIDs),
			attribute.Int("armada.batch.total_jobs", len(metadata.JobIDs)),
		)
	}

	if len(metadata.Operations) > 0 {
		span.SetAttributes(
			attribute.StringSlice("armada.batch.operations", metadata.Operations),
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
