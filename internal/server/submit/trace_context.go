package submit

import (
	"context"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

const traceAnnotationPrefix = "otel.armadaproject.io/"

var traceContextSource = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "armada_trace_context_source_total",
	Help: "Resolved context sources for new jobs; invalid counts rejected carriers before fallback. Only updated when submission trace context is enabled.",
}, []string{"source"})

// resolveSubmissionTraceContext keeps each carrier atomic: trace state and baggage
// may only accompany the traceparent they arrived with. It does no exporting or I/O.
func (s *Server) resolveSubmissionTraceContext(ctx context.Context, item *api.JobSubmitRequestItem) (trace.SpanContext, string) {
	cfg := s.submissionConfig.TraceContext
	if !cfg.Enabled {
		return trace.SpanContext{}, ""
	}
	maxState, maxBaggage := cfg.MaxTraceStateBytes, cfg.MaxBaggageBytes
	if maxState == 0 {
		maxState = configuration.DefaultMaxTraceStateBytes
	}
	if maxBaggage == 0 {
		maxBaggage = configuration.DefaultMaxBaggageBytes
	}
	valid := func(carrier propagation.MapCarrier) (trace.SpanContext, bool) {
		if len(carrier) == 0 {
			return trace.SpanContext{}, false
		}
		if uint(len(carrier["tracestate"])) <= maxState && uint(len(carrier["baggage"])) <= maxBaggage {
			// Extract into an empty context: malformed input must not inherit a server span.
			extracted := propagation.TraceContext{}.Extract(context.Background(), carrier)
			if sc := trace.SpanContextFromContext(extracted); sc.IsValid() {
				return sc, true
			}
		}
		traceContextSource.WithLabelValues("invalid").Inc()
		return trace.SpanContext{}, false
	}
	store := func(carrier propagation.MapCarrier, source string, sc trace.SpanContext) (trace.SpanContext, string) {
		if item.Annotations == nil {
			item.Annotations = make(map[string]string, len(carrier))
		}
		for _, key := range []string{"traceparent", "tracestate", "baggage"} {
			delete(item.Annotations, traceAnnotationPrefix+key)
			if value := carrier[key]; value != "" {
				item.Annotations[traceAnnotationPrefix+key] = value
			}
		}
		traceContextSource.WithLabelValues(source).Inc()
		return sc, source
	}

	carrier := annotationTraceCarrier(item.Annotations, traceAnnotationPrefix)
	if sc, ok := valid(carrier); ok {
		// Keep canonical annotations exactly as supplied, including empty values.
		traceContextSource.WithLabelValues("annotation").Inc()
		return sc, "annotation"
	}
	for _, prefix := range cfg.AliasAnnotationPrefixes {
		carrier = annotationTraceCarrier(item.Annotations, prefix)
		if sc, ok := valid(carrier); ok {
			return store(carrier, "alias", sc)
		}
	}
	carrier = grpcTraceCarrier(ctx, cfg.PropagateGrpcBaggage)
	if sc, ok := valid(carrier); ok {
		linked := submissionLinkContext(ctx, sc)
		if !linked.Equal(sc) {
			updated := propagation.MapCarrier{}
			propagation.TraceContext{}.Inject(trace.ContextWithSpanContext(context.Background(), linked), updated)
			carrier["traceparent"] = updated["traceparent"]
			carrier["tracestate"] = updated["tracestate"]
		}
		sc = linked
		return store(carrier, "grpc", sc)
	}
	// No source was accepted. Do not persist rejected canonical fields for a
	// later consumer to mistake for resolved context; other annotations remain.
	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		delete(item.Annotations, traceAnnotationPrefix+key)
	}
	traceContextSource.WithLabelValues("none").Inc()
	return trace.SpanContext{}, "none"
}

func annotationTraceCarrier(annotations map[string]string, prefix string) propagation.MapCarrier {
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		if value := annotations[prefix+key]; value != "" {
			carrier[key] = value
		}
	}
	return carrier
}

func grpcTraceCarrier(ctx context.Context, propagateBaggage bool) propagation.MapCarrier {
	md, _ := metadata.FromIncomingContext(ctx)
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate", "baggage"} {
		// Ignore unapproved request baggage before allocation and size checks.
		if key == "baggage" && !propagateBaggage {
			continue
		}
		if values := md.Get(key); len(values) > 0 {
			carrier[key] = strings.Join(values, ",")
		}
	}
	return carrier
}

func submissionLinkContext(ctx context.Context, caller trace.SpanContext) trace.SpanContext {
	server := trace.SpanContextFromContext(ctx)
	if server.IsValid() && !server.IsRemote() && server.TraceID() == caller.TraceID() {
		// The server identifies the submit call even when its span is not sampled.
		// Independent job traces retain the caller's sampling decision and state.
		return server.WithTraceFlags(caller.TraceFlags()).WithTraceState(caller.TraceState())
	}
	return caller
}
