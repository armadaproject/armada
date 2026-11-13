package submit

import (
	"context"
	"maps"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/internal/server/submit/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

const (
	canonicalPrefix = "otel.armadaproject.io/"
	parentA         = "00-11111111111111111111111111111111-2222222222222222-01"
	parentB         = "00-33333333333333333333333333333333-4444444444444444-00"
	serverParent    = "00-33333333333333333333333333333333-5555555555555555-00"
)

func TestResolveSubmissionTraceContext(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"traceparent", parentB, "tracestate", "vendor=grpc", "baggage", "origin=grpc"))
	serverSC := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(),
		propagation.MapCarrier{"traceparent": serverParent, "tracestate": "vendor=grpc"}))
	serverSC = serverSC.WithRemote(false)
	withServer := trace.ContextWithSpanContext(incoming, serverSC)
	unrelated := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": parentA})).WithRemote(false)
	largeState, err := trace.ParseTraceState("added=longervalue,vendor=grpc")
	require.NoError(t, err)
	withLargeServerState := trace.ContextWithSpanContext(incoming, serverSC.WithTraceState(largeState))
	cfg := configuration.SubmissionTraceContextConfig{Enabled: true, PropagateGrpcBaggage: true, AliasAnnotationPrefixes: []string{"old/", "older/"}}
	tests := []struct {
		name        string
		ctx         context.Context
		annotations map[string]string
		config      configuration.SubmissionTraceContextConfig
		wantSource  string
		wantParent  string
		wantState   string
		wantBaggage string
		unchanged   bool
	}{
		{
			name:        "disabled preserves invalid input",
			ctx:         incoming,
			annotations: map[string]string{canonicalPrefix + "traceparent": "bad"},
			unchanged:   true,
		},
		{
			name:      "disabled does not allocate annotations",
			ctx:       incoming,
			unchanged: true,
		},
		{
			name:        "canonical wins",
			ctx:         incoming,
			config:      cfg,
			annotations: map[string]string{canonicalPrefix + "traceparent": parentA, canonicalPrefix + "tracestate": "vendor=item", canonicalPrefix + "baggage": "origin=item", "old/traceparent": parentB},
			wantSource:  "annotation",
			wantParent:  parentA,
			wantState:   "vendor=item",
			wantBaggage: "origin=item",
		},
		{
			name:        "aliases checked in order",
			ctx:         incoming,
			config:      cfg,
			annotations: map[string]string{"old/traceparent": parentA, "old/baggage": "origin=alias", "older/traceparent": parentB},
			wantSource:  "alias",
			wantParent:  parentA,
			wantBaggage: "origin=alias",
		},
		{
			name:        "invalid alias continues to next",
			config:      cfg,
			annotations: map[string]string{"old/traceparent": "bad", "older/traceparent": parentB},
			wantSource:  "alias",
			wantParent:  parentB,
		},
		{
			name:        "fallback never mixes carriers",
			config:      cfg,
			annotations: map[string]string{canonicalPrefix + "traceparent": "bad", canonicalPrefix + "tracestate": "vendor=stale", canonicalPrefix + "baggage": "origin=stale", "old/traceparent": parentA},
			wantSource:  "alias",
			wantParent:  parentA,
		},
		{
			name:        "invalid canonical falls back to grpc",
			ctx:         incoming,
			config:      cfg,
			annotations: map[string]string{canonicalPrefix + "traceparent": "bad"},
			wantSource:  "grpc",
			wantParent:  parentB,
			wantState:   "vendor=grpc",
			wantBaggage: "origin=grpc",
		},
		{
			name:        "grpc with no global propagator needed",
			ctx:         incoming,
			config:      cfg,
			wantSource:  "grpc",
			wantParent:  parentB,
			wantState:   "vendor=grpc",
			wantBaggage: "origin=grpc",
		},
		{
			name:        "grpc uses actual server span",
			ctx:         withServer,
			config:      cfg,
			wantSource:  "grpc",
			wantParent:  serverParent,
			wantState:   "vendor=grpc",
			wantBaggage: "origin=grpc",
		},
		{
			name:       "server-only span cannot create job trace",
			ctx:        trace.ContextWithSpanContext(context.Background(), serverSC),
			config:     cfg,
			wantSource: "none",
			unchanged:  true,
		},
		{
			name:       "invalid incoming cannot use server span",
			ctx:        trace.ContextWithSpanContext(metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", "bad")), serverSC),
			config:     cfg,
			wantSource: "none",
			unchanged:  true,
		},
		{
			name:        "unrelated current span does not replace caller",
			ctx:         trace.ContextWithSpanContext(incoming, unrelated),
			config:      cfg,
			wantSource:  "grpc",
			wantParent:  parentB,
			wantState:   "vendor=grpc",
			wantBaggage: "origin=grpc",
		},
		{
			name:       "oversized server state falls back to valid incoming carrier",
			ctx:        withLargeServerState,
			config:     configuration.SubmissionTraceContextConfig{Enabled: true, PropagateGrpcBaggage: true, MaxTraceStateBytes: 11},
			wantSource: "grpc", wantParent: parentB, wantState: "vendor=grpc", wantBaggage: "origin=grpc",
		},
		{
			name:       "empty request",
			config:     cfg,
			wantSource: "none",
			unchanged:  true,
		},
		{
			name:        "default state limit rejects source",
			ctx:         incoming,
			config:      cfg,
			annotations: map[string]string{canonicalPrefix + "traceparent": parentA, canonicalPrefix + "tracestate": strings.Repeat("a", 513)},
			wantSource:  "grpc",
			wantParent:  parentB,
			wantState:   "vendor=grpc",
			wantBaggage: "origin=grpc",
		},
		{
			name:        "default baggage limit rejects source",
			ctx:         incoming,
			config:      cfg,
			annotations: map[string]string{canonicalPrefix + "traceparent": parentA, canonicalPrefix + "baggage": strings.Repeat("a", 8193)},
			wantSource:  "grpc",
			wantParent:  parentB,
			wantState:   "vendor=grpc",
			wantBaggage: "origin=grpc",
		},
		{
			name:        "exact limits accepted",
			config:      configuration.SubmissionTraceContextConfig{Enabled: true, MaxTraceStateBytes: 3, MaxBaggageBytes: 3},
			annotations: map[string]string{canonicalPrefix + "traceparent": parentA, canonicalPrefix + "tracestate": "v=x", canonicalPrefix + "baggage": "k=v"},
			wantSource:  "annotation",
			wantParent:  parentA,
			wantState:   "v=x",
			wantBaggage: "k=v",
		},
		{
			name:       "oversized grpc baggage ignored",
			ctx:        metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parentA, "baggage", "long=value")),
			config:     configuration.SubmissionTraceContextConfig{Enabled: true, PropagateGrpcBaggage: true, MaxBaggageBytes: 3},
			wantSource: "none",
			unchanged:  true,
		},
		{
			name:        "grpc repeated carrier fields joined",
			ctx:         metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parentA, "tracestate", "a=1", "tracestate", "b=2", "baggage", "a=1", "baggage", "b=2")),
			config:      cfg,
			wantSource:  "grpc",
			wantParent:  parentA,
			wantState:   "a=1,b=2",
			wantBaggage: "a=1,b=2",
		},
		{
			name:       "duplicate traceparents rejected",
			ctx:        metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parentA, "traceparent", parentB)),
			config:     cfg,
			wantSource: "none",
			unchanged:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			item := &api.JobSubmitRequestItem{Annotations: maps.Clone(tc.annotations)}
			s := &Server{submissionConfig: configuration.SubmissionConfig{TraceContext: tc.config}}
			sc, source := s.resolveSubmissionTraceContext(ctx, item)
			assert.Equal(t, tc.wantSource, source)
			if tc.unchanged {
				assert.Equal(t, tc.annotations, item.Annotations)
			}
			if tc.wantParent == "" {
				assert.False(t, sc.IsValid())
				return
			}
			assert.Equal(t, tc.wantParent, item.Annotations[canonicalPrefix+"traceparent"])
			assert.Equal(t, tc.wantState, item.Annotations[canonicalPrefix+"tracestate"])
			assert.Equal(t, tc.wantBaggage, item.Annotations[canonicalPrefix+"baggage"])
			wantSC := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{"traceparent": tc.wantParent}))
			assert.Equal(t, wantSC.TraceID(), sc.TraceID())
			assert.Equal(t, wantSC.SpanID(), sc.SpanID())
			assert.Equal(t, wantSC.TraceFlags(), sc.TraceFlags())
			for key, value := range tc.annotations {
				if !strings.HasPrefix(key, canonicalPrefix) {
					assert.Equal(t, value, item.Annotations[key])
				}
			}
		})
	}
}

func TestResolveSubmissionTraceContextMetrics(t *testing.T) {
	s := &Server{submissionConfig: configuration.SubmissionConfig{TraceContext: configuration.SubmissionTraceContextConfig{Enabled: true}}}
	invalidBefore := testutil.ToFloat64(traceContextSource.WithLabelValues("invalid"))
	noneBefore := testutil.ToFloat64(traceContextSource.WithLabelValues("none"))
	_, source := s.resolveSubmissionTraceContext(context.Background(), &api.JobSubmitRequestItem{Annotations: map[string]string{canonicalPrefix + "traceparent": "bad"}})
	require.Equal(t, "none", source)
	assert.Equal(t, invalidBefore+1, testutil.ToFloat64(traceContextSource.WithLabelValues("invalid")))
	assert.Equal(t, noneBefore+1, testutil.ToFloat64(traceContextSource.WithLabelValues("none")))
	s.submissionConfig.TraceContext.Enabled = false
	s.resolveSubmissionTraceContext(context.Background(), &api.JobSubmitRequestItem{Annotations: map[string]string{canonicalPrefix + "traceparent": "bad"}})
	assert.Equal(t, invalidBefore+1, testutil.ToFloat64(traceContextSource.WithLabelValues("invalid")))
	assert.Equal(t, noneBefore+1, testutil.ToFloat64(traceContextSource.WithLabelValues("none")))
}

// Check the real SubmitJobs path and serialized event, rather than just the resolver.
func TestSubmit_TraceContext(t *testing.T) {
	for _, tc := range []struct {
		name             string
		enabled, baggage bool
	}{
		{name: "disabled"}, {name: "enabled without grpc baggage", enabled: true}, {name: "enabled with grpc baggage", enabled: true, baggage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enabled := tc.enabled
			base := armadacontext.WithValue(armadacontext.Background(), "principal", testfixtures.DefaultPrincipal)
			ctx := armadacontext.FromGrpcCtx(metadata.NewIncomingContext(base, metadata.Pairs("traceparent", parentB, "baggage", "origin=grpc")))
			s, mocks := createTestServer(t)
			s.submissionConfig.TraceContext = configuration.SubmissionTraceContextConfig{Enabled: enabled, PropagateGrpcBaggage: tc.baggage}
			req := testfixtures.SubmitRequestWithNItems(3)
			req.JobRequestItems[0].Annotations = map[string]string{canonicalPrefix + "traceparent": parentA}
			req.JobRequestItems[2].Annotations = map[string]string{canonicalPrefix + "traceparent": "invalid-duplicate"}
			mocks.queueRepo.EXPECT().GetQueue(ctx, req.Queue).Return(testfixtures.DefaultQueue, nil)
			mocks.authorizer.EXPECT().AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil)
			mocks.deduplicator.EXPECT().GetOriginalJobIds(ctx, req.Queue, req.JobRequestItems).Return(map[string]string{req.JobRequestItems[2].ClientId: "existing-job"}, nil)
			mocks.deduplicator.EXPECT().StoreOriginalJobIds(ctx, req.Queue, gomock.Any()).Return(nil)
			var published []byte
			mocks.publisher.EXPECT().PublishMessages(ctx, gomock.Any()).DoAndReturn(func(_ *armadacontext.Context, es *armadaevents.EventSequence) error {
				var err error
				published, err = proto.Marshal(es)
				return err
			})
			resp, err := s.SubmitJobs(ctx, req)
			require.NoError(t, err)
			require.Len(t, resp.JobResponseItems, 3)
			assert.Equal(t, "existing-job", resp.JobResponseItems[2].JobId)
			event := &armadaevents.EventSequence{}
			require.NoError(t, proto.Unmarshal(published, event))
			require.Len(t, event.Events, 2)
			first := event.Events[0].GetSubmitJob().ObjectMeta.Annotations
			second := event.Events[1].GetSubmitJob().ObjectMeta.Annotations
			assert.Equal(t, parentA, first[canonicalPrefix+"traceparent"])
			assert.Empty(t, first[canonicalPrefix+"baggage"], "request baggage must not mix into item context")
			if enabled {
				assert.Equal(t, parentB, second[canonicalPrefix+"traceparent"])
				if tc.baggage {
					assert.Equal(t, "origin=grpc", second[canonicalPrefix+"baggage"])
				} else {
					assert.NotContains(t, second, canonicalPrefix+"baggage")
				}
			} else {
				assert.NotContains(t, second, canonicalPrefix+"traceparent")
				assert.NotContains(t, second, canonicalPrefix+"baggage")
			}
			assert.Equal(t, "invalid-duplicate", req.JobRequestItems[2].Annotations[canonicalPrefix+"traceparent"])
		})
	}
}

func TestResolveSubmissionTraceContextGRPCHandler(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	handler := otelgrpc.NewServerHandler(
		otelgrpc.WithTracerProvider(provider),
		otelgrpc.WithPropagators(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})),
	)
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parentA, "tracestate", "vendor=grpc", "baggage", "origin=grpc"))
	ctx := handler.TagRPC(incoming, &stats.RPCTagInfo{FullMethodName: "/api.Submit/SubmitJobs"})
	defer trace.SpanFromContext(ctx).End()
	serverSC := trace.SpanContextFromContext(ctx)
	require.True(t, serverSC.IsValid())
	require.False(t, serverSC.IsRemote())
	item := &api.JobSubmitRequestItem{}
	s := &Server{submissionConfig: configuration.SubmissionConfig{TraceContext: configuration.SubmissionTraceContextConfig{Enabled: true, PropagateGrpcBaggage: true}}}
	sc, source := s.resolveSubmissionTraceContext(ctx, item)
	require.Equal(t, "grpc", source)
	assert.Equal(t, serverSC, sc)
	assert.NotEqual(t, "2222222222222222", sc.SpanID().String(), "use the server child, rather than the incoming client span")
	assert.Equal(t, "11111111111111111111111111111111", sc.TraceID().String())
	assert.Equal(t, "vendor=grpc", item.Annotations[canonicalPrefix+"tracestate"])
	assert.Equal(t, "origin=grpc", item.Annotations[canonicalPrefix+"baggage"])
}

func TestSubmit_TraceContextLiteralBaggage(t *testing.T) {
	for _, source := range []string{"canonical", "alias", "grpc", "disabled"} {
		t.Run(source, func(t *testing.T) {
			const baggage = "origin={JobId}"
			base := armadacontext.WithValue(armadacontext.Background(), "principal", testfixtures.DefaultPrincipal)
			ctx := armadacontext.FromGrpcCtx(metadata.NewIncomingContext(base, metadata.Pairs("traceparent", parentB, "baggage", baggage)))
			s, m := createTestServer(t)
			s.submissionConfig.TraceContext = configuration.SubmissionTraceContextConfig{Enabled: source != "disabled", PropagateGrpcBaggage: true, AliasAnnotationPrefixes: []string{"legacy/"}, MaxBaggageBytes: uint(len(baggage))}
			req := testfixtures.SubmitRequestWithNItems(1)
			annotations := map[string]string{"custom": "id={JobId}"}
			if source != "grpc" {
				prefix := canonicalPrefix
				if source == "alias" {
					prefix = "legacy/"
				}
				annotations[prefix+"traceparent"] = parentB
				annotations[prefix+"baggage"] = baggage
			}
			req.JobRequestItems[0].Annotations = annotations
			m.queueRepo.EXPECT().GetQueue(ctx, req.Queue).Return(testfixtures.DefaultQueue, nil)
			m.authorizer.EXPECT().AuthorizeQueueAction(ctx, testfixtures.DefaultQueue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit).Return(nil)
			m.deduplicator.EXPECT().GetOriginalJobIds(ctx, req.Queue, req.JobRequestItems).Return(nil, nil)
			m.deduplicator.EXPECT().StoreOriginalJobIds(ctx, req.Queue, gomock.Any()).Return(nil)
			var published []byte
			m.publisher.EXPECT().PublishMessages(ctx, gomock.Any()).DoAndReturn(func(_ *armadacontext.Context, es *armadaevents.EventSequence) error {
				var err error
				published, err = proto.Marshal(es)
				return err
			})
			resp, err := s.SubmitJobs(ctx, req)
			require.NoError(t, err)
			event := &armadaevents.EventSequence{}
			require.NoError(t, proto.Unmarshal(published, event))
			result := event.Events[0].GetSubmitJob().ObjectMeta.Annotations
			assert.Equal(t, "id="+resp.JobResponseItems[0].JobId, result["custom"])
			if source == "disabled" {
				assert.Equal(t, "origin="+resp.JobResponseItems[0].JobId, result[canonicalPrefix+"baggage"])
			} else {
				assert.Equal(t, baggage, result[canonicalPrefix+"baggage"])
				assert.LessOrEqual(t, len(result[canonicalPrefix+"baggage"]), len(baggage), "conversion must not expand validated baggage")
			}
		})
	}
}

func TestResolveSubmissionTraceContextGrpcBaggagePolicy(t *testing.T) {
	for _, tc := range []struct {
		name, baggage, wantSource, wantBaggage string
		propagate                              bool
		annotations                            map[string]string
	}{
		{name: "default excludes grpc baggage", baggage: "k=v", wantSource: "grpc"},
		{name: "default ignores oversized grpc baggage", baggage: "oversized", wantSource: "grpc"},
		{name: "opt in forwards exact limit", baggage: "k=v", propagate: true, wantSource: "grpc", wantBaggage: "k=v"},
		{name: "opt in rejects oversized carrier", baggage: "oversized", propagate: true, wantSource: "none"},
		{name: "canonical baggage stays explicit", baggage: "mesh=value", wantSource: "annotation", wantBaggage: "k=v", annotations: map[string]string{canonicalPrefix + "traceparent": parentA, canonicalPrefix + "baggage": "k=v"}},
		{name: "alias baggage stays explicit", baggage: "mesh=value", wantSource: "alias", wantBaggage: "k=v", annotations: map[string]string{"old/traceparent": parentA, "old/baggage": "k=v"}},
		{name: "fallback clears stale canonical baggage", baggage: "mesh=value", wantSource: "grpc", annotations: map[string]string{canonicalPrefix + "traceparent": "bad", canonicalPrefix + "baggage": "old"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{submissionConfig: configuration.SubmissionConfig{TraceContext: configuration.SubmissionTraceContextConfig{Enabled: true, PropagateGrpcBaggage: tc.propagate, MaxBaggageBytes: 3, AliasAnnotationPrefixes: []string{"old/"}}}}
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("traceparent", parentB, "tracestate", "v=x", "baggage", tc.baggage))
			item := &api.JobSubmitRequestItem{Annotations: maps.Clone(tc.annotations)}
			sc, source := s.resolveSubmissionTraceContext(ctx, item)
			assert.Equal(t, tc.wantSource, source)
			assert.Equal(t, tc.wantSource != "none", sc.IsValid())
			assert.Equal(t, tc.wantBaggage, item.Annotations[canonicalPrefix+"baggage"])
			if tc.wantSource == "grpc" {
				assert.Equal(t, parentB, item.Annotations[canonicalPrefix+"traceparent"])
				assert.Equal(t, "v=x", item.Annotations[canonicalPrefix+"tracestate"])
				if !tc.propagate {
					assert.NotContains(t, item.Annotations, canonicalPrefix+"baggage")
				}
			}
		})
	}
}
