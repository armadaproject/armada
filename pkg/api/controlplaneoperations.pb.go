// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pkg/api/controlplaneoperations.proto

package api

import (
	context "context"
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type ExecutorCordonRequest struct {
	Name   string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Reason string `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
}

func (m *ExecutorCordonRequest) Reset()         { *m = ExecutorCordonRequest{} }
func (m *ExecutorCordonRequest) String() string { return proto.CompactTextString(m) }
func (*ExecutorCordonRequest) ProtoMessage()    {}
func (*ExecutorCordonRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dd33f51e3bfc603, []int{0}
}
func (m *ExecutorCordonRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ExecutorCordonRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ExecutorCordonRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ExecutorCordonRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExecutorCordonRequest.Merge(m, src)
}
func (m *ExecutorCordonRequest) XXX_Size() int {
	return m.Size()
}
func (m *ExecutorCordonRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ExecutorCordonRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ExecutorCordonRequest proto.InternalMessageInfo

func (m *ExecutorCordonRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ExecutorCordonRequest) GetReason() string {
	if m != nil {
		return m.Reason
	}
	return ""
}

type ExecutorUncordonRequest struct {
	Name   string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Reason string `protobuf:"bytes,2,opt,name=reason,proto3" json:"reason,omitempty"`
}

func (m *ExecutorUncordonRequest) Reset()         { *m = ExecutorUncordonRequest{} }
func (m *ExecutorUncordonRequest) String() string { return proto.CompactTextString(m) }
func (*ExecutorUncordonRequest) ProtoMessage()    {}
func (*ExecutorUncordonRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8dd33f51e3bfc603, []int{1}
}
func (m *ExecutorUncordonRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ExecutorUncordonRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ExecutorUncordonRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ExecutorUncordonRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExecutorUncordonRequest.Merge(m, src)
}
func (m *ExecutorUncordonRequest) XXX_Size() int {
	return m.Size()
}
func (m *ExecutorUncordonRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ExecutorUncordonRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ExecutorUncordonRequest proto.InternalMessageInfo

func (m *ExecutorUncordonRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ExecutorUncordonRequest) GetReason() string {
	if m != nil {
		return m.Reason
	}
	return ""
}

func init() {
	proto.RegisterType((*ExecutorCordonRequest)(nil), "api.ExecutorCordonRequest")
	proto.RegisterType((*ExecutorUncordonRequest)(nil), "api.ExecutorUncordonRequest")
}

func init() {
	proto.RegisterFile("pkg/api/controlplaneoperations.proto", fileDescriptor_8dd33f51e3bfc603)
}

var fileDescriptor_8dd33f51e3bfc603 = []byte{
	// 350 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xb4, 0x92, 0x41, 0x4b, 0x32, 0x41,
	0x18, 0xc7, 0x5d, 0xdf, 0x17, 0xa1, 0x39, 0x88, 0x4c, 0x69, 0xb2, 0xc9, 0x28, 0x4b, 0x54, 0x84,
	0xcc, 0x50, 0xdd, 0xba, 0xa5, 0x78, 0x2e, 0x84, 0x2e, 0xdd, 0xc6, 0x75, 0xda, 0xd6, 0xdc, 0x79,
	0xa6, 0x71, 0x36, 0xea, 0xda, 0x27, 0x08, 0xfa, 0x52, 0x1d, 0x85, 0x2e, 0x9d, 0x24, 0xb4, 0x93,
	0x9f, 0x22, 0x9c, 0xdd, 0x15, 0x8d, 0x3a, 0x76, 0x7c, 0xf6, 0xf9, 0xef, 0xfc, 0xe6, 0xff, 0x63,
	0xd0, 0xae, 0xba, 0x0d, 0x18, 0x57, 0x21, 0xf3, 0x41, 0x1a, 0x0d, 0x43, 0x35, 0xe4, 0x52, 0x80,
	0x12, 0x9a, 0x9b, 0x10, 0xe4, 0x88, 0x2a, 0x0d, 0x06, 0xf0, 0x3f, 0xae, 0x42, 0xb7, 0x16, 0x00,
	0x04, 0x43, 0x61, 0xd3, 0x5c, 0x4a, 0x30, 0xab, 0x11, 0x77, 0x27, 0xdd, 0xda, 0xa9, 0x17, 0x5f,
	0x33, 0x11, 0x29, 0xf3, 0x98, 0x2c, 0xbd, 0x08, 0x95, 0x3b, 0x0f, 0xc2, 0x8f, 0x0d, 0xe8, 0x36,
	0xe8, 0x3e, 0xc8, 0xae, 0xb8, 0x8b, 0xc5, 0xc8, 0xe0, 0x3d, 0xf4, 0x5f, 0xf2, 0x48, 0x54, 0x9d,
	0x86, 0x73, 0xb0, 0xd1, 0xc2, 0xf3, 0x49, 0xbd, 0xb8, 0x98, 0x9b, 0x10, 0x85, 0xc6, 0x1e, 0xd0,
	0xb5, 0x7b, 0xdc, 0x44, 0x05, 0x2d, 0xf8, 0x08, 0x64, 0x35, 0x6f, 0x93, 0x5b, 0xf3, 0x49, 0xbd,
	0x94, 0x7c, 0x59, 0xc9, 0xa6, 0x19, 0x0f, 0xd0, 0x76, 0x86, 0xbb, 0x94, 0xfe, 0xdf, 0x03, 0x8f,
	0xe7, 0x0e, 0xaa, 0xb4, 0x13, 0x81, 0x17, 0x0b, 0x81, 0xe7, 0x4b, 0x81, 0xb8, 0x8f, 0x8a, 0x49,
	0xe5, 0xec, 0x46, 0xd8, 0xa5, 0x5c, 0x85, 0xf4, 0x47, 0x1f, 0x6e, 0x85, 0x26, 0x1a, 0x69, 0xa6,
	0x91, 0x76, 0x16, 0x20, 0x8f, 0x3c, 0xbd, 0x7d, 0xbe, 0xe4, 0xab, 0xde, 0x26, 0xbb, 0x3f, 0x62,
	0x22, 0xfd, 0x95, 0x25, 0xd5, 0x4e, 0x9d, 0x43, 0x3c, 0x40, 0xa5, 0xac, 0xe9, 0x92, 0x53, 0x5b,
	0xe3, 0x7c, 0x13, 0xf1, 0x2b, 0xa9, 0x61, 0x49, 0xae, 0x57, 0x5e, 0x23, 0xc5, 0x72, 0xc9, 0x6a,
	0x9d, 0xbd, 0x4e, 0x89, 0x33, 0x9e, 0x12, 0xe7, 0x63, 0x4a, 0x9c, 0xe7, 0x19, 0xc9, 0x8d, 0x67,
	0x24, 0xf7, 0x3e, 0x23, 0xb9, 0xab, 0xfd, 0x20, 0x34, 0x37, 0x71, 0x8f, 0xfa, 0x10, 0x31, 0xae,
	0x23, 0xde, 0xe7, 0x4a, 0xc3, 0x40, 0xf8, 0x26, 0x9d, 0x58, 0xfa, 0xd8, 0x7a, 0x05, 0x0b, 0x3d,
	0xf9, 0x0a, 0x00, 0x00, 0xff, 0xff, 0xc9, 0x0e, 0x09, 0x5a, 0x7e, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ControlPlaneOperationsClient is the client API for ControlPlaneOperations service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ControlPlaneOperationsClient interface {
	CordonExecutor(ctx context.Context, in *ExecutorCordonRequest, opts ...grpc.CallOption) (*types.Empty, error)
	UncordonExecutor(ctx context.Context, in *ExecutorUncordonRequest, opts ...grpc.CallOption) (*types.Empty, error)
}

type controlPlaneOperationsClient struct {
	cc *grpc.ClientConn
}

func NewControlPlaneOperationsClient(cc *grpc.ClientConn) ControlPlaneOperationsClient {
	return &controlPlaneOperationsClient{cc}
}

func (c *controlPlaneOperationsClient) CordonExecutor(ctx context.Context, in *ExecutorCordonRequest, opts ...grpc.CallOption) (*types.Empty, error) {
	out := new(types.Empty)
	err := c.cc.Invoke(ctx, "/api.ControlPlaneOperations/CordonExecutor", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controlPlaneOperationsClient) UncordonExecutor(ctx context.Context, in *ExecutorUncordonRequest, opts ...grpc.CallOption) (*types.Empty, error) {
	out := new(types.Empty)
	err := c.cc.Invoke(ctx, "/api.ControlPlaneOperations/UncordonExecutor", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ControlPlaneOperationsServer is the server API for ControlPlaneOperations service.
type ControlPlaneOperationsServer interface {
	CordonExecutor(context.Context, *ExecutorCordonRequest) (*types.Empty, error)
	UncordonExecutor(context.Context, *ExecutorUncordonRequest) (*types.Empty, error)
}

// UnimplementedControlPlaneOperationsServer can be embedded to have forward compatible implementations.
type UnimplementedControlPlaneOperationsServer struct {
}

func (*UnimplementedControlPlaneOperationsServer) CordonExecutor(ctx context.Context, req *ExecutorCordonRequest) (*types.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CordonExecutor not implemented")
}
func (*UnimplementedControlPlaneOperationsServer) UncordonExecutor(ctx context.Context, req *ExecutorUncordonRequest) (*types.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UncordonExecutor not implemented")
}

func RegisterControlPlaneOperationsServer(s *grpc.Server, srv ControlPlaneOperationsServer) {
	s.RegisterService(&_ControlPlaneOperations_serviceDesc, srv)
}

func _ControlPlaneOperations_CordonExecutor_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutorCordonRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlPlaneOperationsServer).CordonExecutor(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ControlPlaneOperations/CordonExecutor",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlPlaneOperationsServer).CordonExecutor(ctx, req.(*ExecutorCordonRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControlPlaneOperations_UncordonExecutor_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ExecutorUncordonRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControlPlaneOperationsServer).UncordonExecutor(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.ControlPlaneOperations/UncordonExecutor",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControlPlaneOperationsServer).UncordonExecutor(ctx, req.(*ExecutorUncordonRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _ControlPlaneOperations_serviceDesc = grpc.ServiceDesc{
	ServiceName: "api.ControlPlaneOperations",
	HandlerType: (*ControlPlaneOperationsServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CordonExecutor",
			Handler:    _ControlPlaneOperations_CordonExecutor_Handler,
		},
		{
			MethodName: "UncordonExecutor",
			Handler:    _ControlPlaneOperations_UncordonExecutor_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/api/controlplaneoperations.proto",
}

func (m *ExecutorCordonRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ExecutorCordonRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ExecutorCordonRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Reason) > 0 {
		i -= len(m.Reason)
		copy(dAtA[i:], m.Reason)
		i = encodeVarintControlplaneoperations(dAtA, i, uint64(len(m.Reason)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintControlplaneoperations(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *ExecutorUncordonRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ExecutorUncordonRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ExecutorUncordonRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Reason) > 0 {
		i -= len(m.Reason)
		copy(dAtA[i:], m.Reason)
		i = encodeVarintControlplaneoperations(dAtA, i, uint64(len(m.Reason)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintControlplaneoperations(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintControlplaneoperations(dAtA []byte, offset int, v uint64) int {
	offset -= sovControlplaneoperations(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *ExecutorCordonRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovControlplaneoperations(uint64(l))
	}
	l = len(m.Reason)
	if l > 0 {
		n += 1 + l + sovControlplaneoperations(uint64(l))
	}
	return n
}

func (m *ExecutorUncordonRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovControlplaneoperations(uint64(l))
	}
	l = len(m.Reason)
	if l > 0 {
		n += 1 + l + sovControlplaneoperations(uint64(l))
	}
	return n
}

func sovControlplaneoperations(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozControlplaneoperations(x uint64) (n int) {
	return sovControlplaneoperations(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *ExecutorCordonRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowControlplaneoperations
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ExecutorCordonRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExecutorCordonRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowControlplaneoperations
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Reason", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowControlplaneoperations
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Reason = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipControlplaneoperations(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *ExecutorUncordonRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowControlplaneoperations
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: ExecutorUncordonRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExecutorUncordonRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowControlplaneoperations
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Reason", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowControlplaneoperations
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Reason = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipControlplaneoperations(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthControlplaneoperations
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipControlplaneoperations(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowControlplaneoperations
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowControlplaneoperations
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowControlplaneoperations
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthControlplaneoperations
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupControlplaneoperations
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthControlplaneoperations
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthControlplaneoperations        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowControlplaneoperations          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupControlplaneoperations = fmt.Errorf("proto: unexpected end of group")
)
