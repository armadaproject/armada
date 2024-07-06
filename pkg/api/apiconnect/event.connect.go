// Code generated by protoc-gen-connect-go. DO NOT EDIT.
//
// Source: api/event.proto

package apiconnect

import (
	connect "connectrpc.com/connect"
	context "context"
	errors "errors"
	api "github.com/armadaproject/armada/pkg/api"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	http "net/http"
	strings "strings"
)

// This is a compile-time assertion to ensure that this generated file and the connect package are
// compatible. If you get a compiler error that this constant is not defined, this code was
// generated with a version of connect newer than the one compiled into your binary. You can fix the
// problem by either regenerating this code with an older version of connect or updating the connect
// version compiled into your binary.
const _ = connect.IsAtLeastVersion1_13_0

const (
	// EventName is the fully-qualified name of the Event service.
	EventName = "api.Event"
)

// These constants are the fully-qualified names of the RPCs defined in this package. They're
// exposed at runtime as Spec.Procedure and as the final two segments of the HTTP route.
//
// Note that these are different from the fully-qualified method names used by
// google.golang.org/protobuf/reflect/protoreflect. To convert from these constants to
// reflection-formatted method names, remove the leading slash and convert the remaining slash to a
// period.
const (
	// EventGetJobSetEventsProcedure is the fully-qualified name of the Event's GetJobSetEvents RPC.
	EventGetJobSetEventsProcedure = "/api.Event/GetJobSetEvents"
	// EventWatchProcedure is the fully-qualified name of the Event's Watch RPC.
	EventWatchProcedure = "/api.Event/Watch"
	// EventHealthProcedure is the fully-qualified name of the Event's Health RPC.
	EventHealthProcedure = "/api.Event/Health"
)

// These variables are the protoreflect.Descriptor objects for the RPCs defined in this package.
var (
	eventServiceDescriptor               = api.File_api_event_proto.Services().ByName("Event")
	eventGetJobSetEventsMethodDescriptor = eventServiceDescriptor.Methods().ByName("GetJobSetEvents")
	eventWatchMethodDescriptor           = eventServiceDescriptor.Methods().ByName("Watch")
	eventHealthMethodDescriptor          = eventServiceDescriptor.Methods().ByName("Health")
)

// EventClient is a client for the api.Event service.
type EventClient interface {
	GetJobSetEvents(context.Context, *connect.Request[api.JobSetRequest]) (*connect.ServerStreamForClient[api.EventStreamMessage], error)
	// Deprecated: do not use.
	Watch(context.Context, *connect.Request[api.WatchRequest]) (*connect.ServerStreamForClient[api.EventStreamMessage], error)
	Health(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[api.HealthCheckResponse], error)
}

// NewEventClient constructs a client for the api.Event service. By default, it uses the Connect
// protocol with the binary Protobuf Codec, asks for gzipped responses, and sends uncompressed
// requests. To use the gRPC or gRPC-Web protocols, supply the connect.WithGRPC() or
// connect.WithGRPCWeb() options.
//
// The URL supplied here should be the base URL for the Connect or gRPC server (for example,
// http://api.acme.com or https://acme.com/grpc).
func NewEventClient(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) EventClient {
	baseURL = strings.TrimRight(baseURL, "/")
	return &eventClient{
		getJobSetEvents: connect.NewClient[api.JobSetRequest, api.EventStreamMessage](
			httpClient,
			baseURL+EventGetJobSetEventsProcedure,
			connect.WithSchema(eventGetJobSetEventsMethodDescriptor),
			connect.WithClientOptions(opts...),
		),
		watch: connect.NewClient[api.WatchRequest, api.EventStreamMessage](
			httpClient,
			baseURL+EventWatchProcedure,
			connect.WithSchema(eventWatchMethodDescriptor),
			connect.WithClientOptions(opts...),
		),
		health: connect.NewClient[emptypb.Empty, api.HealthCheckResponse](
			httpClient,
			baseURL+EventHealthProcedure,
			connect.WithSchema(eventHealthMethodDescriptor),
			connect.WithClientOptions(opts...),
		),
	}
}

// eventClient implements EventClient.
type eventClient struct {
	getJobSetEvents *connect.Client[api.JobSetRequest, api.EventStreamMessage]
	watch           *connect.Client[api.WatchRequest, api.EventStreamMessage]
	health          *connect.Client[emptypb.Empty, api.HealthCheckResponse]
}

// GetJobSetEvents calls api.Event.GetJobSetEvents.
func (c *eventClient) GetJobSetEvents(ctx context.Context, req *connect.Request[api.JobSetRequest]) (*connect.ServerStreamForClient[api.EventStreamMessage], error) {
	return c.getJobSetEvents.CallServerStream(ctx, req)
}

// Watch calls api.Event.Watch.
//
// Deprecated: do not use.
func (c *eventClient) Watch(ctx context.Context, req *connect.Request[api.WatchRequest]) (*connect.ServerStreamForClient[api.EventStreamMessage], error) {
	return c.watch.CallServerStream(ctx, req)
}

// Health calls api.Event.Health.
func (c *eventClient) Health(ctx context.Context, req *connect.Request[emptypb.Empty]) (*connect.Response[api.HealthCheckResponse], error) {
	return c.health.CallUnary(ctx, req)
}

// EventHandler is an implementation of the api.Event service.
type EventHandler interface {
	GetJobSetEvents(context.Context, *connect.Request[api.JobSetRequest], *connect.ServerStream[api.EventStreamMessage]) error
	// Deprecated: do not use.
	Watch(context.Context, *connect.Request[api.WatchRequest], *connect.ServerStream[api.EventStreamMessage]) error
	Health(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[api.HealthCheckResponse], error)
}

// NewEventHandler builds an HTTP handler from the service implementation. It returns the path on
// which to mount the handler and the handler itself.
//
// By default, handlers support the Connect, gRPC, and gRPC-Web protocols with the binary Protobuf
// and JSON codecs. They also support gzip compression.
func NewEventHandler(svc EventHandler, opts ...connect.HandlerOption) (string, http.Handler) {
	eventGetJobSetEventsHandler := connect.NewServerStreamHandler(
		EventGetJobSetEventsProcedure,
		svc.GetJobSetEvents,
		connect.WithSchema(eventGetJobSetEventsMethodDescriptor),
		connect.WithHandlerOptions(opts...),
	)
	eventWatchHandler := connect.NewServerStreamHandler(
		EventWatchProcedure,
		svc.Watch,
		connect.WithSchema(eventWatchMethodDescriptor),
		connect.WithHandlerOptions(opts...),
	)
	eventHealthHandler := connect.NewUnaryHandler(
		EventHealthProcedure,
		svc.Health,
		connect.WithSchema(eventHealthMethodDescriptor),
		connect.WithHandlerOptions(opts...),
	)
	return "/api.Event/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case EventGetJobSetEventsProcedure:
			eventGetJobSetEventsHandler.ServeHTTP(w, r)
		case EventWatchProcedure:
			eventWatchHandler.ServeHTTP(w, r)
		case EventHealthProcedure:
			eventHealthHandler.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

// UnimplementedEventHandler returns CodeUnimplemented from all methods.
type UnimplementedEventHandler struct{}

func (UnimplementedEventHandler) GetJobSetEvents(context.Context, *connect.Request[api.JobSetRequest], *connect.ServerStream[api.EventStreamMessage]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.New("api.Event.GetJobSetEvents is not implemented"))
}

func (UnimplementedEventHandler) Watch(context.Context, *connect.Request[api.WatchRequest], *connect.ServerStream[api.EventStreamMessage]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.New("api.Event.Watch is not implemented"))
}

func (UnimplementedEventHandler) Health(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[api.HealthCheckResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("api.Event.Health is not implemented"))
}
