// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pkg/api/executor.proto

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

type ExecutorSettingsUpsertRequest struct {
	Name         string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Cordoned     bool   `protobuf:"varint,2,opt,name=cordoned,proto3" json:"cordoned,omitempty"`
	CordonReason string `protobuf:"bytes,3,opt,name=cordonReason,proto3" json:"cordonReason,omitempty"`
}

func (m *ExecutorSettingsUpsertRequest) Reset()         { *m = ExecutorSettingsUpsertRequest{} }
func (m *ExecutorSettingsUpsertRequest) String() string { return proto.CompactTextString(m) }
func (*ExecutorSettingsUpsertRequest) ProtoMessage()    {}
func (*ExecutorSettingsUpsertRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_506cd9cd149291ea, []int{0}
}
func (m *ExecutorSettingsUpsertRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ExecutorSettingsUpsertRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ExecutorSettingsUpsertRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ExecutorSettingsUpsertRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExecutorSettingsUpsertRequest.Merge(m, src)
}
func (m *ExecutorSettingsUpsertRequest) XXX_Size() int {
	return m.Size()
}
func (m *ExecutorSettingsUpsertRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ExecutorSettingsUpsertRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ExecutorSettingsUpsertRequest proto.InternalMessageInfo

func (m *ExecutorSettingsUpsertRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ExecutorSettingsUpsertRequest) GetCordoned() bool {
	if m != nil {
		return m.Cordoned
	}
	return false
}

func (m *ExecutorSettingsUpsertRequest) GetCordonReason() string {
	if m != nil {
		return m.CordonReason
	}
	return ""
}

type ExecutorSettingsDeleteRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *ExecutorSettingsDeleteRequest) Reset()         { *m = ExecutorSettingsDeleteRequest{} }
func (m *ExecutorSettingsDeleteRequest) String() string { return proto.CompactTextString(m) }
func (*ExecutorSettingsDeleteRequest) ProtoMessage()    {}
func (*ExecutorSettingsDeleteRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_506cd9cd149291ea, []int{1}
}
func (m *ExecutorSettingsDeleteRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ExecutorSettingsDeleteRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ExecutorSettingsDeleteRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ExecutorSettingsDeleteRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExecutorSettingsDeleteRequest.Merge(m, src)
}
func (m *ExecutorSettingsDeleteRequest) XXX_Size() int {
	return m.Size()
}
func (m *ExecutorSettingsDeleteRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_ExecutorSettingsDeleteRequest.DiscardUnknown(m)
}

var xxx_messageInfo_ExecutorSettingsDeleteRequest proto.InternalMessageInfo

func (m *ExecutorSettingsDeleteRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func init() {
	proto.RegisterType((*ExecutorSettingsUpsertRequest)(nil), "api.ExecutorSettingsUpsertRequest")
	proto.RegisterType((*ExecutorSettingsDeleteRequest)(nil), "api.ExecutorSettingsDeleteRequest")
}

func init() { proto.RegisterFile("pkg/api/executor.proto", fileDescriptor_506cd9cd149291ea) }

var fileDescriptor_506cd9cd149291ea = []byte{
	// 369 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x52, 0x3d, 0x4f, 0x02, 0x41,
	0x10, 0x65, 0xc1, 0x18, 0xbc, 0x18, 0x8b, 0x2d, 0x2e, 0xe4, 0xc4, 0x03, 0xaf, 0x10, 0x42, 0xcc,
	0x6d, 0xc4, 0xce, 0xc2, 0x44, 0x22, 0xb1, 0xc7, 0xd8, 0xd8, 0x98, 0xe5, 0x18, 0xcf, 0x55, 0x6e,
	0x77, 0xbd, 0xdb, 0x33, 0x1a, 0x43, 0x63, 0x65, 0x69, 0xe2, 0xcf, 0xf1, 0x0f, 0x58, 0x92, 0xd8,
	0x58, 0x11, 0x03, 0x56, 0xfc, 0x0a, 0x73, 0x1f, 0x10, 0xd0, 0x44, 0xb4, 0xdb, 0x99, 0xf7, 0xf6,
	0xbd, 0x99, 0xb7, 0xab, 0xe9, 0xf2, 0xca, 0x25, 0x54, 0x32, 0x02, 0xb7, 0xe0, 0x84, 0x4a, 0xf8,
	0xb6, 0xf4, 0x85, 0x12, 0x38, 0x47, 0x25, 0x33, 0x8a, 0xae, 0x10, 0x6e, 0x17, 0x62, 0x9c, 0x72,
	0x2e, 0x14, 0x55, 0x4c, 0xf0, 0x20, 0xa1, 0x18, 0xeb, 0x29, 0x1a, 0x57, 0xed, 0xf0, 0x9c, 0x80,
	0x27, 0xd5, 0x5d, 0x02, 0x5a, 0x2f, 0x48, 0xdb, 0x68, 0xa6, 0x92, 0xc7, 0xa0, 0x14, 0xe3, 0x6e,
	0x70, 0x22, 0x03, 0xf0, 0x55, 0x0b, 0xae, 0x43, 0x08, 0x14, 0xde, 0xd2, 0x96, 0x38, 0xf5, 0xa0,
	0x80, 0xca, 0xa8, 0xba, 0xd2, 0xc0, 0xe3, 0x41, 0x69, 0x2d, 0xaa, 0xb7, 0x85, 0xc7, 0x54, 0xac,
	0xd4, 0x8a, 0x71, 0x5c, 0xd7, 0xf2, 0x8e, 0xf0, 0x3b, 0x82, 0x43, 0xa7, 0x90, 0x2d, 0xa3, 0x6a,
	0xbe, 0xa1, 0x8f, 0x07, 0x25, 0x3c, 0xe9, 0xcd, 0xf0, 0xa7, 0x3c, 0xbc, 0xaf, 0xad, 0x26, 0xe7,
	0x16, 0xd0, 0x40, 0xf0, 0x42, 0x2e, 0xf6, 0x30, 0xc6, 0x83, 0x92, 0x3e, 0xdb, 0x9f, 0xb9, 0x3b,
	0xc7, 0xb7, 0x8e, 0x7e, 0x0e, 0x7f, 0x08, 0x5d, 0x50, 0xf0, 0xcf, 0xe1, 0xeb, 0x8f, 0x59, 0x2d,
	0x3f, 0x51, 0xc2, 0x3d, 0x4d, 0x4f, 0x22, 0xf8, 0xae, 0x8d, 0x2d, 0x9b, 0x4a, 0x66, 0xff, 0x9a,
	0x97, 0xa1, 0xdb, 0x49, 0xde, 0xf6, 0x24, 0x6f, 0xbb, 0x19, 0x19, 0x59, 0x95, 0x87, 0xb7, 0xcf,
	0xe7, 0xec, 0xa6, 0x51, 0x24, 0x37, 0x3b, 0xd3, 0x57, 0x3c, 0x0b, 0x52, 0x0d, 0x72, 0x1f, 0xcd,
	0xd1, 0xdb, 0x43, 0xb5, 0xc8, 0x3e, 0x59, 0xe2, 0x8f, 0xf6, 0x73, 0x1b, 0x2f, 0xb2, 0xaf, 0x2d,
	0xb2, 0x6f, 0x1c, 0xbc, 0x0e, 0x4d, 0xd4, 0x1f, 0x9a, 0xe8, 0x63, 0x68, 0xa2, 0xa7, 0x91, 0x99,
	0xe9, 0x8f, 0xcc, 0xcc, 0xfb, 0xc8, 0xcc, 0x9c, 0x56, 0x5c, 0xa6, 0x2e, 0xc2, 0xb6, 0xed, 0x08,
	0x8f, 0x50, 0xdf, 0xa3, 0x1d, 0x2a, 0x7d, 0x71, 0x09, 0x8e, 0x4a, 0x2b, 0x92, 0xfe, 0xd1, 0xf6,
	0x72, 0xec, 0xbd, 0xfb, 0x15, 0x00, 0x00, 0xff, 0xff, 0x63, 0xab, 0xb0, 0x13, 0xb5, 0x02, 0x00,
	0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// ExecutorClient is the client API for Executor service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type ExecutorClient interface {
	UpsertExecutorSettings(ctx context.Context, in *ExecutorSettingsUpsertRequest, opts ...grpc.CallOption) (*types.Empty, error)
	DeleteExecutorSettings(ctx context.Context, in *ExecutorSettingsDeleteRequest, opts ...grpc.CallOption) (*types.Empty, error)
}

type executorClient struct {
	cc *grpc.ClientConn
}

func NewExecutorClient(cc *grpc.ClientConn) ExecutorClient {
	return &executorClient{cc}
}

func (c *executorClient) UpsertExecutorSettings(ctx context.Context, in *ExecutorSettingsUpsertRequest, opts ...grpc.CallOption) (*types.Empty, error) {
	out := new(types.Empty)
	err := c.cc.Invoke(ctx, "/api.Executor/UpsertExecutorSettings", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *executorClient) DeleteExecutorSettings(ctx context.Context, in *ExecutorSettingsDeleteRequest, opts ...grpc.CallOption) (*types.Empty, error) {
	out := new(types.Empty)
	err := c.cc.Invoke(ctx, "/api.Executor/DeleteExecutorSettings", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ExecutorServer is the server API for Executor service.
type ExecutorServer interface {
	UpsertExecutorSettings(context.Context, *ExecutorSettingsUpsertRequest) (*types.Empty, error)
	DeleteExecutorSettings(context.Context, *ExecutorSettingsDeleteRequest) (*types.Empty, error)
}

// UnimplementedExecutorServer can be embedded to have forward compatible implementations.
type UnimplementedExecutorServer struct {
}

func (*UnimplementedExecutorServer) UpsertExecutorSettings(ctx context.Context, req *ExecutorSettingsUpsertRequest) (*types.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpsertExecutorSettings not implemented")
}
func (*UnimplementedExecutorServer) DeleteExecutorSettings(ctx context.Context, req *ExecutorSettingsDeleteRequest) (*types.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteExecutorSettings not implemented")
}

func RegisterExecutorServer(s *grpc.Server, srv ExecutorServer) {
	s.RegisterService(&_Executor_serviceDesc, srv)
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
		FullMethod: "/api.Executor/UpsertExecutorSettings",
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
		FullMethod: "/api.Executor/DeleteExecutorSettings",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ExecutorServer).DeleteExecutorSettings(ctx, req.(*ExecutorSettingsDeleteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Executor_serviceDesc = grpc.ServiceDesc{
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
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/api/executor.proto",
}

func (m *ExecutorSettingsUpsertRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ExecutorSettingsUpsertRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ExecutorSettingsUpsertRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.CordonReason) > 0 {
		i -= len(m.CordonReason)
		copy(dAtA[i:], m.CordonReason)
		i = encodeVarintExecutor(dAtA, i, uint64(len(m.CordonReason)))
		i--
		dAtA[i] = 0x1a
	}
	if m.Cordoned {
		i--
		if m.Cordoned {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x10
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintExecutor(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *ExecutorSettingsDeleteRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ExecutorSettingsDeleteRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ExecutorSettingsDeleteRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintExecutor(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintExecutor(dAtA []byte, offset int, v uint64) int {
	offset -= sovExecutor(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *ExecutorSettingsUpsertRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovExecutor(uint64(l))
	}
	if m.Cordoned {
		n += 2
	}
	l = len(m.CordonReason)
	if l > 0 {
		n += 1 + l + sovExecutor(uint64(l))
	}
	return n
}

func (m *ExecutorSettingsDeleteRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovExecutor(uint64(l))
	}
	return n
}

func sovExecutor(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozExecutor(x uint64) (n int) {
	return sovExecutor(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *ExecutorSettingsUpsertRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowExecutor
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
			return fmt.Errorf("proto: ExecutorSettingsUpsertRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExecutorSettingsUpsertRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowExecutor
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
				return ErrInvalidLengthExecutor
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthExecutor
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Cordoned", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowExecutor
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Cordoned = bool(v != 0)
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CordonReason", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowExecutor
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
				return ErrInvalidLengthExecutor
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthExecutor
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.CordonReason = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipExecutor(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthExecutor
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
func (m *ExecutorSettingsDeleteRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowExecutor
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
			return fmt.Errorf("proto: ExecutorSettingsDeleteRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExecutorSettingsDeleteRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowExecutor
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
				return ErrInvalidLengthExecutor
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthExecutor
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipExecutor(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthExecutor
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
func skipExecutor(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowExecutor
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
					return 0, ErrIntOverflowExecutor
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
					return 0, ErrIntOverflowExecutor
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
				return 0, ErrInvalidLengthExecutor
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupExecutor
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthExecutor
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthExecutor        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowExecutor          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupExecutor = fmt.Errorf("proto: unexpected end of group")
)
