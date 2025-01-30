// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pkg/priorityoverride/prioritymultiplier.proto

package priorityoverride

import (
	context "context"
	encoding_binary "encoding/binary"
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
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

type PriorityMultiplierRequest struct {
}

func (m *PriorityMultiplierRequest) Reset()         { *m = PriorityMultiplierRequest{} }
func (m *PriorityMultiplierRequest) String() string { return proto.CompactTextString(m) }
func (*PriorityMultiplierRequest) ProtoMessage()    {}
func (*PriorityMultiplierRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_8d112cf26c3c73e1, []int{0}
}
func (m *PriorityMultiplierRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PriorityMultiplierRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PriorityMultiplierRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PriorityMultiplierRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PriorityMultiplierRequest.Merge(m, src)
}
func (m *PriorityMultiplierRequest) XXX_Size() int {
	return m.Size()
}
func (m *PriorityMultiplierRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PriorityMultiplierRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PriorityMultiplierRequest proto.InternalMessageInfo

type PoolPriorityMultipliers struct {
	Pool        string             `protobuf:"bytes,1,opt,name=pool,proto3" json:"pool,omitempty"`
	Multipliers map[string]float64 `protobuf:"bytes,2,rep,name=multipliers,proto3" json:"multipliers,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
}

func (m *PoolPriorityMultipliers) Reset()         { *m = PoolPriorityMultipliers{} }
func (m *PoolPriorityMultipliers) String() string { return proto.CompactTextString(m) }
func (*PoolPriorityMultipliers) ProtoMessage()    {}
func (*PoolPriorityMultipliers) Descriptor() ([]byte, []int) {
	return fileDescriptor_8d112cf26c3c73e1, []int{1}
}
func (m *PoolPriorityMultipliers) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PoolPriorityMultipliers) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PoolPriorityMultipliers.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PoolPriorityMultipliers) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PoolPriorityMultipliers.Merge(m, src)
}
func (m *PoolPriorityMultipliers) XXX_Size() int {
	return m.Size()
}
func (m *PoolPriorityMultipliers) XXX_DiscardUnknown() {
	xxx_messageInfo_PoolPriorityMultipliers.DiscardUnknown(m)
}

var xxx_messageInfo_PoolPriorityMultipliers proto.InternalMessageInfo

func (m *PoolPriorityMultipliers) GetPool() string {
	if m != nil {
		return m.Pool
	}
	return ""
}

func (m *PoolPriorityMultipliers) GetMultipliers() map[string]float64 {
	if m != nil {
		return m.Multipliers
	}
	return nil
}

type PriorityMultiplierResponse struct {
	PoolPriorityMultipliers []*PoolPriorityMultipliers `protobuf:"bytes,1,rep,name=poolPriorityMultipliers,proto3" json:"poolPriorityMultipliers,omitempty"`
}

func (m *PriorityMultiplierResponse) Reset()         { *m = PriorityMultiplierResponse{} }
func (m *PriorityMultiplierResponse) String() string { return proto.CompactTextString(m) }
func (*PriorityMultiplierResponse) ProtoMessage()    {}
func (*PriorityMultiplierResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_8d112cf26c3c73e1, []int{2}
}
func (m *PriorityMultiplierResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PriorityMultiplierResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PriorityMultiplierResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PriorityMultiplierResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PriorityMultiplierResponse.Merge(m, src)
}
func (m *PriorityMultiplierResponse) XXX_Size() int {
	return m.Size()
}
func (m *PriorityMultiplierResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_PriorityMultiplierResponse.DiscardUnknown(m)
}

var xxx_messageInfo_PriorityMultiplierResponse proto.InternalMessageInfo

func (m *PriorityMultiplierResponse) GetPoolPriorityMultipliers() []*PoolPriorityMultipliers {
	if m != nil {
		return m.PoolPriorityMultipliers
	}
	return nil
}

func init() {
	proto.RegisterType((*PriorityMultiplierRequest)(nil), "api.PriorityMultiplierRequest")
	proto.RegisterType((*PoolPriorityMultipliers)(nil), "api.PoolPriorityMultipliers")
	proto.RegisterMapType((map[string]float64)(nil), "api.PoolPriorityMultipliers.MultipliersEntry")
	proto.RegisterType((*PriorityMultiplierResponse)(nil), "api.PriorityMultiplierResponse")
}

func init() {
	proto.RegisterFile("pkg/priorityoverride/prioritymultiplier.proto", fileDescriptor_8d112cf26c3c73e1)
}

var fileDescriptor_8d112cf26c3c73e1 = []byte{
	// 349 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x7c, 0x52, 0x3d, 0x4f, 0xc3, 0x30,
	0x10, 0xad, 0x53, 0x40, 0xc2, 0x15, 0x5f, 0xe6, 0xa3, 0x69, 0x41, 0x76, 0x29, 0x02, 0x15, 0x89,
	0x06, 0xa9, 0x2c, 0x88, 0x31, 0x12, 0x62, 0x42, 0xaa, 0xca, 0x04, 0x5b, 0x81, 0x03, 0x99, 0xa6,
	0xb5, 0x71, 0xdc, 0x48, 0x19, 0xf8, 0x0f, 0x2c, 0x4c, 0xfc, 0x21, 0xc6, 0x8e, 0x4c, 0x11, 0x6a,
	0xb7, 0xfc, 0x0a, 0xd4, 0xb4, 0x2a, 0x16, 0x24, 0x6c, 0xf6, 0xdd, 0xf3, 0xbb, 0xf7, 0x7c, 0x0f,
	0xd7, 0x65, 0xe7, 0xf1, 0x58, 0x2a, 0x2e, 0x14, 0xd7, 0xa1, 0x08, 0x40, 0x29, 0x7e, 0x0f, 0xb3,
	0x42, 0xb7, 0xef, 0x69, 0x2e, 0x3d, 0x0e, 0xca, 0x91, 0x4a, 0x68, 0x41, 0xf2, 0x6d, 0xc9, 0xab,
	0xdb, 0xb8, 0xd4, 0x9c, 0x02, 0x2e, 0x67, 0x80, 0x16, 0x3c, 0xf7, 0xc1, 0xd7, 0xd5, 0x37, 0x0b,
	0x17, 0x9b, 0x42, 0x78, 0x7f, 0x11, 0x3e, 0x39, 0xc0, 0x73, 0x52, 0x08, 0xcf, 0x46, 0x15, 0x54,
	0x5b, 0x74, 0x49, 0x1c, 0xb1, 0xe5, 0xf1, 0xfd, 0x48, 0x74, 0xb9, 0x86, 0xae, 0xd4, 0x61, 0x2b,
	0xe9, 0x93, 0x27, 0x5c, 0xf8, 0x99, 0xec, 0xdb, 0x56, 0x25, 0x5f, 0x2b, 0x34, 0xea, 0x4e, 0x5b,
	0x72, 0x27, 0x83, 0xda, 0x31, 0xce, 0xe7, 0x3d, 0xad, 0x42, 0xb7, 0x14, 0x47, 0x6c, 0xd3, 0x60,
	0x31, 0x86, 0x98, 0xe4, 0xe5, 0x07, 0xbc, 0xfa, 0xfb, 0x2d, 0xd9, 0xc3, 0xf9, 0x0e, 0x84, 0x53,
	0x99, 0x6b, 0x71, 0xc4, 0x96, 0x3a, 0x10, 0x1a, 0x04, 0xe3, 0x2e, 0x39, 0xc4, 0xf3, 0x41, 0xdb,
	0xeb, 0x83, 0x6d, 0x55, 0x50, 0x0d, 0xb9, 0xeb, 0x71, 0xc4, 0x56, 0x92, 0x82, 0x01, 0x9c, 0x20,
	0xce, 0xac, 0x53, 0x54, 0x7d, 0x47, 0xb8, 0x9c, 0xf6, 0x6b, 0xbe, 0x14, 0x3d, 0x1f, 0xc8, 0x0b,
	0x2e, 0xca, 0x74, 0x6b, 0x36, 0x4a, 0xec, 0xef, 0xfc, 0x67, 0xdf, 0xdd, 0x8f, 0x23, 0xb6, 0x9b,
	0x41, 0x60, 0xe8, 0xc9, 0x9a, 0xd1, 0x08, 0xd2, 0x56, 0x7a, 0x05, 0x2a, 0xe0, 0x77, 0x40, 0xae,
	0xf1, 0xd6, 0x05, 0xe8, 0xb4, 0x85, 0xd2, 0x89, 0xa8, 0xac, 0x30, 0x94, 0x59, 0x66, 0x7f, 0x62,
	0xdb, 0x75, 0x3e, 0x86, 0x14, 0x0d, 0x86, 0x14, 0x7d, 0x0d, 0x29, 0x7a, 0x1d, 0xd1, 0xdc, 0x60,
	0x44, 0x73, 0x9f, 0x23, 0x9a, 0xbb, 0xd9, 0x48, 0x0b, 0xe6, 0xed, 0x42, 0x12, 0xc3, 0x93, 0xef,
	0x00, 0x00, 0x00, 0xff, 0xff, 0x8c, 0x8a, 0xca, 0xdd, 0xb7, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// PriorityMultiplierServiceClient is the client API for PriorityMultiplierService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type PriorityMultiplierServiceClient interface {
	GetPriorityMultipliers(ctx context.Context, in *PriorityMultiplierRequest, opts ...grpc.CallOption) (*PriorityMultiplierResponse, error)
}

type priorityMultiplierServiceClient struct {
	cc *grpc.ClientConn
}

func NewPriorityMultiplierServiceClient(cc *grpc.ClientConn) PriorityMultiplierServiceClient {
	return &priorityMultiplierServiceClient{cc}
}

func (c *priorityMultiplierServiceClient) GetPriorityMultipliers(ctx context.Context, in *PriorityMultiplierRequest, opts ...grpc.CallOption) (*PriorityMultiplierResponse, error) {
	out := new(PriorityMultiplierResponse)
	err := c.cc.Invoke(ctx, "/api.PriorityMultiplierService/GetPriorityMultipliers", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PriorityMultiplierServiceServer is the server API for PriorityMultiplierService service.
type PriorityMultiplierServiceServer interface {
	GetPriorityMultipliers(context.Context, *PriorityMultiplierRequest) (*PriorityMultiplierResponse, error)
}

// UnimplementedPriorityMultiplierServiceServer can be embedded to have forward compatible implementations.
type UnimplementedPriorityMultiplierServiceServer struct {
}

func (*UnimplementedPriorityMultiplierServiceServer) GetPriorityMultipliers(ctx context.Context, req *PriorityMultiplierRequest) (*PriorityMultiplierResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetPriorityMultipliers not implemented")
}

func RegisterPriorityMultiplierServiceServer(s *grpc.Server, srv PriorityMultiplierServiceServer) {
	s.RegisterService(&_PriorityMultiplierService_serviceDesc, srv)
}

func _PriorityMultiplierService_GetPriorityMultipliers_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PriorityMultiplierRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PriorityMultiplierServiceServer).GetPriorityMultipliers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/api.PriorityMultiplierService/GetPriorityMultipliers",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PriorityMultiplierServiceServer).GetPriorityMultipliers(ctx, req.(*PriorityMultiplierRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _PriorityMultiplierService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "api.PriorityMultiplierService",
	HandlerType: (*PriorityMultiplierServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetPriorityMultipliers",
			Handler:    _PriorityMultiplierService_GetPriorityMultipliers_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/priorityoverride/prioritymultiplier.proto",
}

func (m *PriorityMultiplierRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PriorityMultiplierRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PriorityMultiplierRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	return len(dAtA) - i, nil
}

func (m *PoolPriorityMultipliers) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PoolPriorityMultipliers) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PoolPriorityMultipliers) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Multipliers) > 0 {
		for k := range m.Multipliers {
			v := m.Multipliers[k]
			baseI := i
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(v))))
			i--
			dAtA[i] = 0x11
			i -= len(k)
			copy(dAtA[i:], k)
			i = encodeVarintPrioritymultiplier(dAtA, i, uint64(len(k)))
			i--
			dAtA[i] = 0xa
			i = encodeVarintPrioritymultiplier(dAtA, i, uint64(baseI-i))
			i--
			dAtA[i] = 0x12
		}
	}
	if len(m.Pool) > 0 {
		i -= len(m.Pool)
		copy(dAtA[i:], m.Pool)
		i = encodeVarintPrioritymultiplier(dAtA, i, uint64(len(m.Pool)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *PriorityMultiplierResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PriorityMultiplierResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PriorityMultiplierResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.PoolPriorityMultipliers) > 0 {
		for iNdEx := len(m.PoolPriorityMultipliers) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.PoolPriorityMultipliers[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintPrioritymultiplier(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func encodeVarintPrioritymultiplier(dAtA []byte, offset int, v uint64) int {
	offset -= sovPrioritymultiplier(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *PriorityMultiplierRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	return n
}

func (m *PoolPriorityMultipliers) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Pool)
	if l > 0 {
		n += 1 + l + sovPrioritymultiplier(uint64(l))
	}
	if len(m.Multipliers) > 0 {
		for k, v := range m.Multipliers {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovPrioritymultiplier(uint64(len(k))) + 1 + 8
			n += mapEntrySize + 1 + sovPrioritymultiplier(uint64(mapEntrySize))
		}
	}
	return n
}

func (m *PriorityMultiplierResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.PoolPriorityMultipliers) > 0 {
		for _, e := range m.PoolPriorityMultipliers {
			l = e.Size()
			n += 1 + l + sovPrioritymultiplier(uint64(l))
		}
	}
	return n
}

func sovPrioritymultiplier(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozPrioritymultiplier(x uint64) (n int) {
	return sovPrioritymultiplier(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *PriorityMultiplierRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPrioritymultiplier
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
			return fmt.Errorf("proto: PriorityMultiplierRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PriorityMultiplierRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		default:
			iNdEx = preIndex
			skippy, err := skipPrioritymultiplier(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthPrioritymultiplier
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
func (m *PoolPriorityMultipliers) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPrioritymultiplier
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
			return fmt.Errorf("proto: PoolPriorityMultipliers: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PoolPriorityMultipliers: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Pool", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPrioritymultiplier
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
				return ErrInvalidLengthPrioritymultiplier
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthPrioritymultiplier
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Pool = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Multipliers", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPrioritymultiplier
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPrioritymultiplier
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthPrioritymultiplier
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Multipliers == nil {
				m.Multipliers = make(map[string]float64)
			}
			var mapkey string
			var mapvalue float64
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowPrioritymultiplier
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
				if fieldNum == 1 {
					var stringLenmapkey uint64
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowPrioritymultiplier
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						stringLenmapkey |= uint64(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					intStringLenmapkey := int(stringLenmapkey)
					if intStringLenmapkey < 0 {
						return ErrInvalidLengthPrioritymultiplier
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey < 0 {
						return ErrInvalidLengthPrioritymultiplier
					}
					if postStringIndexmapkey > l {
						return io.ErrUnexpectedEOF
					}
					mapkey = string(dAtA[iNdEx:postStringIndexmapkey])
					iNdEx = postStringIndexmapkey
				} else if fieldNum == 2 {
					var mapvaluetemp uint64
					if (iNdEx + 8) > l {
						return io.ErrUnexpectedEOF
					}
					mapvaluetemp = uint64(encoding_binary.LittleEndian.Uint64(dAtA[iNdEx:]))
					iNdEx += 8
					mapvalue = math.Float64frombits(mapvaluetemp)
				} else {
					iNdEx = entryPreIndex
					skippy, err := skipPrioritymultiplier(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if (skippy < 0) || (iNdEx+skippy) < 0 {
						return ErrInvalidLengthPrioritymultiplier
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.Multipliers[mapkey] = mapvalue
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPrioritymultiplier(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthPrioritymultiplier
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
func (m *PriorityMultiplierResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowPrioritymultiplier
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
			return fmt.Errorf("proto: PriorityMultiplierResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PriorityMultiplierResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field PoolPriorityMultipliers", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowPrioritymultiplier
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthPrioritymultiplier
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthPrioritymultiplier
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.PoolPriorityMultipliers = append(m.PoolPriorityMultipliers, &PoolPriorityMultipliers{})
			if err := m.PoolPriorityMultipliers[len(m.PoolPriorityMultipliers)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipPrioritymultiplier(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthPrioritymultiplier
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
func skipPrioritymultiplier(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowPrioritymultiplier
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
					return 0, ErrIntOverflowPrioritymultiplier
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
					return 0, ErrIntOverflowPrioritymultiplier
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
				return 0, ErrInvalidLengthPrioritymultiplier
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupPrioritymultiplier
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthPrioritymultiplier
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthPrioritymultiplier        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowPrioritymultiplier          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupPrioritymultiplier = fmt.Errorf("proto: unexpected end of group")
)
