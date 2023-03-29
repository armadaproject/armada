package tracing

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel/propagation"
)

func SerializeTrace(ctx context.Context) (*types.Struct, error) {
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	jsonCarrier, _ := json.Marshal(carrier)
	serializedCarrier := &types.Struct{}
	if err := jsonpb.Unmarshal(bytes.NewReader(jsonCarrier), serializedCarrier); err != nil {
		return nil, err
	}
	return serializedCarrier, nil
}

func ExtractTraceCtx(ctx context.Context, carrier *types.Struct) (context.Context, error) {
	if carrier == nil {
		return ctx, nil
	}
	var buf bytes.Buffer
	marshaler := jsonpb.Marshaler{}
	if err := marshaler.Marshal(&buf, carrier); err != nil {
		return ctx, err
	}
	var mapCarrier propagation.MapCarrier
	if err := json.Unmarshal(buf.Bytes(), &mapCarrier); err != nil {
		return ctx, err
	}
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	traceCtx := propagator.Extract(ctx, mapCarrier)
	return traceCtx, nil
}

func ExtractTraceparent(ctx context.Context) string {
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier["traceparent"]
}
