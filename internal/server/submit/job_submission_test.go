package submit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/observability"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/internal/server/submit/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestSubmit_JobSpans(t *testing.T) {
	const sampledB = "00-33333333333333333333333333333333-4444444444444444-01"
	for _, tc := range []struct {
		name                                                                                      string
		disabled, noProvider, unsampled, absent, duplicate, publishFailure, invalid, dedupFailure bool
	}{
		{name: "accepted"},
		{name: "disabled", disabled: true},
		{name: "server tracing off", noProvider: true},
		{name: "unsampled", unsampled: true},
		{name: "absent", absent: true},
		{name: "duplicate", duplicate: true},
		{name: "publish failure", publishFailure: true},
		{name: "invalid request", invalid: true},
		{name: "dedup store failure", dedupFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(
				sdktrace.WithSyncer(exporter), sdktrace.WithIDGenerator(observability.JobSpanIDGenerator{}),
				sdktrace.WithSampler(observability.JobSpanSampler{Fallback: sdktrace.NeverSample()}),
				sdktrace.WithSpanProcessor(observability.NewSpanAttributePolicyProcessor(nil)),
			)
			old := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			if tc.noProvider {
				otel.SetTracerProvider(noop.NewTracerProvider())
			}
			t.Cleanup(func() { otel.SetTracerProvider(old); require.NoError(t, provider.Shutdown(context.Background())) })
			ctx := armadacontext.WithValue(armadacontext.Background(), "principal", testfixtures.DefaultPrincipal)
			s, m := createTestServer(t)
			s.submissionConfig.TraceContext = configuration.SubmissionTraceContextConfig{Enabled: !tc.disabled, AliasAnnotationPrefixes: []string{"legacy/"}}
			start := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
			clock := clocktesting.NewFakeClock(start)
			s.clock = clock
			// Fixed ULIDs independently decoded as 01563e3ab5d3d6764c61efb99302bd5b/c.
			ids := []string{"01ARZ3NDEKTSV4RRFFQ69G5FAV", "01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAX"}
			n := 0
			s.idGenerator = func() string { id := ids[n]; n++; return id }
			req := testfixtures.SubmitRequestWithNItems(3)
			parent := parentA
			if tc.unsampled {
				parent = parent[:len(parent)-2] + "00"
			}
			req.JobRequestItems[0].Annotations = map[string]string{canonicalPrefix + "traceparent": parent, canonicalPrefix + "tracestate": "vendor=value,ot=th:0"}
			req.JobRequestItems[1].Annotations = map[string]string{"legacy/traceparent": sampledB}
			// A distinct request context verifies per-item precedence and the grpc source.
			ctx = armadacontext.FromGrpcCtx(metadata.NewIncomingContext(ctx, metadata.Pairs("traceparent", sampledB)))
			if tc.absent {
				for _, item := range req.JobRequestItems {
					item.Annotations = nil
				}
				ctx = armadacontext.WithValue(armadacontext.Background(), "principal", testfixtures.DefaultPrincipal)
			}
			if tc.unsampled {
				req.JobRequestItems[1].Annotations = map[string]string{canonicalPrefix + "traceparent": parent}
				req.JobRequestItems[2].Annotations = map[string]string{canonicalPrefix + "traceparent": parent}
			}
			m.queueRepo.EXPECT().GetQueue(ctx, req.Queue).Return(testfixtures.DefaultQueue, nil)
			m.authorizer.EXPECT().AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil)
			if tc.invalid {
				req.JobRequestItems[0].PodSpec = nil
				req.JobRequestItems[0].PodSpecs = nil
			} else {
				duplicates := map[string]string{}
				if tc.duplicate {
					for _, item := range req.JobRequestItems {
						duplicates[item.ClientId] = "existing-job"
					}
				}
				m.deduplicator.EXPECT().GetOriginalJobIds(ctx, req.Queue, req.JobRequestItems).Return(duplicates, nil)
				if !tc.duplicate {
					m.publisher.EXPECT().PublishMessages(ctx, gomock.Any()).DoAndReturn(func(_ *armadacontext.Context, es *armadaevents.EventSequence) error {
						assert.Empty(t, exporter.GetSpans(), "no span before acceptance")
						if !tc.disabled && !tc.absent {
							assert.Equal(t, parent, es.Events[0].GetSubmitJob().ObjectMeta.Annotations[canonicalPrefix+"traceparent"])
						}
						clock.Step(2 * time.Second)
						if tc.publishFailure {
							return errors.New("publication failed")
						}
						return nil
					})
					if !tc.publishFailure {
						var err error
						if tc.dedupFailure {
							err = errors.New("dedup store failed")
						}
						m.deduplicator.EXPECT().StoreOriginalJobIds(ctx, req.Queue, gomock.Any()).Return(err)
					}
				}
			}
			resp, err := s.SubmitJobs(ctx, req)
			if tc.publishFailure || tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, resp.JobResponseItems, 3)
			}
			spans := exporter.GetSpans()
			if tc.disabled || tc.noProvider || tc.unsampled || tc.absent || tc.duplicate || tc.publishFailure || tc.invalid {
				require.Empty(t, spans)
				return
			}
			require.Len(t, spans, 3)
			for i, span := range spans {
				assert.Equal(t, "armada.job.submit", span.Name)
				assert.False(t, span.Parent.IsValid(), "job span must be a root")
				expectedTrace := []string{"01563e3ab5d3d6764c61efb99302bd5b", "01563e3ab5d3d6764c61efb99302bd5c", "01563e3ab5d3d6764c61efb99302bd5d"}[i]
				assert.Equal(t, expectedTrace, span.SpanContext.TraceID().String())
				assert.Equal(t, expectedTrace[16:], span.SpanContext.SpanID().String())
				assert.True(t, span.SpanContext.IsSampled())
				assert.Equal(t, start, span.StartTime)
				assert.Equal(t, start.Add(2*time.Second), span.EndTime)
				assert.ElementsMatch(t, []attribute.KeyValue{attribute.String("armada.queue", req.Queue), attribute.String("armada.job_set.id", req.JobSetId), attribute.String("armada.job.id", ids[i])}, span.Attributes)
				require.Len(t, span.Links, 1)
				target := sampledB
				state := ""
				if i == 0 {
					target = parentA
					state = "vendor=value,ot=th:0"
					assert.Equal(t, "ot=th:0", span.SpanContext.TraceState().String())
				} else {
					assert.Empty(t, span.SpanContext.TraceState().String())
				}
				extracted := propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": target, "tracestate": state})
				assert.Equal(t, trace.SpanContextFromContext(extracted), span.Links[0].SpanContext)
				assert.Equal(t, []attribute.KeyValue{attribute.String("armada.trace_context.source", []string{"annotation", "alias", "grpc"}[i])}, span.Links[0].Attributes)
			}
		})
	}
}
