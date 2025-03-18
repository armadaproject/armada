// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.29.3
// source: pkg/api/executor.proto

package api

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	Executor_UpsertExecutorSettings_FullMethodName = "/api.Executor/UpsertExecutorSettings"
	Executor_DeleteExecutorSettings_FullMethodName = "/api.Executor/DeleteExecutorSettings"
	Executor_PreemptOnExecutor_FullMethodName      = "/api.Executor/PreemptOnExecutor"
	Executor_CancelOnExecutor_FullMethodName       = "/api.Executor/CancelOnExecutor"
)

// ExecutorClient is the client API for Executor service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ExecutorClient interface {
	UpsertExecutorSettings(ctx context.Context, in *ExecutorSettingsUpsertRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	DeleteExecutorSettings(ctx context.Context, in *ExecutorSettingsDeleteRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	PreemptOnExecutor(ctx context.Context, in *ExecutorPreemptRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	CancelOnExecutor(ctx context.Context, in *ExecutorCancelRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type executorClient struct {
	cc grpc.ClientConnInterface
}

func NewExecutorClient(cc grpc.ClientConnInterface) ExecutorClient {
	return &executorClient{cc}
}

func (c *executorClient) UpsertExecutorSettings(ctx context.Context, in *ExecutorSettingsUpsertRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Executor_UpsertExecutorSettings_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executorClient) DeleteExecutorSettings(ctx context.Context, in *ExecutorSettingsDeleteRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Executor_DeleteExecutorSettings_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executorClient) PreemptOnExecutor(ctx context.Context, in *ExecutorPreemptRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Executor_PreemptOnExecutor_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executorClient) CancelOnExecutor(ctx context.Context, in *ExecutorCancelRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, Executor_CancelOnExecutor_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExecutorServer is the server API for Executor service.
// All implementations must embed UnimplementedExecutorServer
// for forward compatibility.
type ExecutorServer interface {
	UpsertExecutorSettings(context.Context, *ExecutorSettingsUpsertRequest) (*emptypb.Empty, error)
	DeleteExecutorSettings(context.Context, *ExecutorSettingsDeleteRequest) (*emptypb.Empty, error)
	PreemptOnExecutor(context.Context, *ExecutorPreemptRequest) (*emptypb.Empty, error)
	CancelOnExecutor(context.Context, *ExecutorCancelRequest) (*emptypb.Empty, error)
	mustEmbedUnimplementedExecutorServer()
}

// UnimplementedExecutorServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedExecutorServer struct{}

func (UnimplementedExecutorServer) UpsertExecutorSettings(context.Context, *ExecutorSettingsUpsertRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpsertExecutorSettings not implemented")
}
func (UnimplementedExecutorServer) DeleteExecutorSettings(context.Context, *ExecutorSettingsDeleteRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteExecutorSettings not implemented")
}
func (UnimplementedExecutorServer) PreemptOnExecutor(context.Context, *ExecutorPreemptRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PreemptOnExecutor not implemented")
}
func (UnimplementedExecutorServer) CancelOnExecutor(context.Context, *ExecutorCancelRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CancelOnExecutor not implemented")
}
func (UnimplementedExecutorServer) mustEmbedUnimplementedExecutorServer() {}
func (UnimplementedExecutorServer) testEmbeddedByValue()                  {}

// UnsafeExecutorServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ExecutorServer will
// result in compilation errors.
type UnsafeExecutorServer interface {
	mustEmbedUnimplementedExecutorServer()
}

func RegisterExecutorServer(s grpc.ServiceRegistrar, srv ExecutorServer) {
	// If the following call pancis, it indicates UnimplementedExecutorServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&Executor_ServiceDesc, srv)
}

func _Executor_UpsertExecutorSettings_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutorSettingsUpsertRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutorServer).UpsertExecutorSettings(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Executor_UpsertExecutorSettings_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutorServer).UpsertExecutorSettings(ctx, req.(*ExecutorSettingsUpsertRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Executor_DeleteExecutorSettings_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutorSettingsDeleteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutorServer).DeleteExecutorSettings(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Executor_DeleteExecutorSettings_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutorServer).DeleteExecutorSettings(ctx, req.(*ExecutorSettingsDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Executor_PreemptOnExecutor_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutorPreemptRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutorServer).PreemptOnExecutor(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Executor_PreemptOnExecutor_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutorServer).PreemptOnExecutor(ctx, req.(*ExecutorPreemptRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Executor_CancelOnExecutor_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutorCancelRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ExecutorServer).CancelOnExecutor(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Executor_CancelOnExecutor_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutorServer).CancelOnExecutor(ctx, req.(*ExecutorCancelRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Executor_ServiceDesc is the grpc.ServiceDesc for Executor service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Executor_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "api.Executor",
	HandlerType: (*ExecutorServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "UpsertExecutorSettings",
			Handler:    _Executor_UpsertExecutorSettings_Handler,
		},
		{
			MethodName: "DeleteExecutorSettings",
			Handler:    _Executor_DeleteExecutorSettings_Handler,
		},
		{
			MethodName: "PreemptOnExecutor",
			Handler:    _Executor_PreemptOnExecutor_Handler,
		},
		{
			MethodName: "CancelOnExecutor",
			Handler:    _Executor_CancelOnExecutor_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/api/executor.proto",
}
