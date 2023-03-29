// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pkg/api/jobservice/jobservice.proto

package jobservice

import (
	context "context"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	io "io"
	math "math"
	math_bits "math/bits"
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

type HealthCheckResponse_ServingStatus int32

const (
	HealthCheckResponse_UNKNOWN     HealthCheckResponse_ServingStatus = 0
	HealthCheckResponse_SERVING     HealthCheckResponse_ServingStatus = 1
	HealthCheckResponse_NOT_SERVING HealthCheckResponse_ServingStatus = 2
)

var HealthCheckResponse_ServingStatus_name = map[int32]string{
	0: "UNKNOWN",
	1: "SERVING",
	2: "NOT_SERVING",
}

var HealthCheckResponse_ServingStatus_value = map[string]int32{
	"UNKNOWN":     0,
	"SERVING":     1,
	"NOT_SERVING": 2,
}

func (x HealthCheckResponse_ServingStatus) String() string {
	return proto.EnumName(HealthCheckResponse_ServingStatus_name, int32(x))
}

func (HealthCheckResponse_ServingStatus) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_acaf6279d0169157, []int{0, 0}
}

// See event.proto for a list of possible events
// We have filtered down these events to only the important ones for a user
type JobServiceResponse_State int32

const (
	JobServiceResponse_SUBMITTED        JobServiceResponse_State = 0
	JobServiceResponse_DUPLICATE_FOUND  JobServiceResponse_State = 1
	JobServiceResponse_RUNNING          JobServiceResponse_State = 2
	JobServiceResponse_FAILED           JobServiceResponse_State = 3
	JobServiceResponse_SUCCEEDED        JobServiceResponse_State = 4
	JobServiceResponse_CANCELLED        JobServiceResponse_State = 5
	JobServiceResponse_JOB_ID_NOT_FOUND JobServiceResponse_State = 6
	JobServiceResponse_CONNECTION_ERR   JobServiceResponse_State = 7
)

var JobServiceResponse_State_name = map[int32]string{
	0: "SUBMITTED",
	1: "DUPLICATE_FOUND",
	2: "RUNNING",
	3: "FAILED",
	4: "SUCCEEDED",
	5: "CANCELLED",
	6: "JOB_ID_NOT_FOUND",
	7: "CONNECTION_ERR",
}

var JobServiceResponse_State_value = map[string]int32{
	"SUBMITTED":        0,
	"DUPLICATE_FOUND":  1,
	"RUNNING":          2,
	"FAILED":           3,
	"SUCCEEDED":        4,
	"CANCELLED":        5,
	"JOB_ID_NOT_FOUND": 6,
	"CONNECTION_ERR":   7,
}

func (x JobServiceResponse_State) String() string {
	return proto.EnumName(JobServiceResponse_State_name, int32(x))
}

func (JobServiceResponse_State) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_acaf6279d0169157, []int{2, 0}
}

type HealthCheckResponse struct {
	Status HealthCheckResponse_ServingStatus `protobuf:"varint,1,opt,name=status,proto3,enum=jobservice.HealthCheckResponse_ServingStatus" json:"status,omitempty"`
}

func (m *HealthCheckResponse) Reset()         { *m = HealthCheckResponse{} }
func (m *HealthCheckResponse) String() string { return proto.CompactTextString(m) }
func (*HealthCheckResponse) ProtoMessage()    {}
func (*HealthCheckResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_acaf6279d0169157, []int{0}
}
func (m *HealthCheckResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *HealthCheckResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_HealthCheckResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *HealthCheckResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_HealthCheckResponse.Merge(m, src)
}
func (m *HealthCheckResponse) XXX_Size() int {
	return m.Size()
}
func (m *HealthCheckResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_HealthCheckResponse.DiscardUnknown(m)
}

var xxx_messageInfo_HealthCheckResponse proto.InternalMessageInfo

func (m *HealthCheckResponse) GetStatus() HealthCheckResponse_ServingStatus {
	if m != nil {
		return m.Status
	}
	return HealthCheckResponse_UNKNOWN
}

type JobServiceRequest struct {
	JobId    string `protobuf:"bytes,1,opt,name=job_id,json=jobId,proto3" json:"jobId,omitempty"`
	JobSetId string `protobuf:"bytes,2,opt,name=job_set_id,json=jobSetId,proto3" json:"jobSetId,omitempty"`
	Queue    string `protobuf:"bytes,3,opt,name=queue,proto3" json:"queue,omitempty"`
}

func (m *JobServiceRequest) Reset()         { *m = JobServiceRequest{} }
func (m *JobServiceRequest) String() string { return proto.CompactTextString(m) }
func (*JobServiceRequest) ProtoMessage()    {}
func (*JobServiceRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_acaf6279d0169157, []int{1}
}
func (m *JobServiceRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JobServiceRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JobServiceRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *JobServiceRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobServiceRequest.Merge(m, src)
}
func (m *JobServiceRequest) XXX_Size() int {
	return m.Size()
}
func (m *JobServiceRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_JobServiceRequest.DiscardUnknown(m)
}

var xxx_messageInfo_JobServiceRequest proto.InternalMessageInfo

func (m *JobServiceRequest) GetJobId() string {
	if m != nil {
		return m.JobId
	}
	return ""
}

func (m *JobServiceRequest) GetJobSetId() string {
	if m != nil {
		return m.JobSetId
	}
	return ""
}

func (m *JobServiceRequest) GetQueue() string {
	if m != nil {
		return m.Queue
	}
	return ""
}

type JobServiceResponse struct {
	State JobServiceResponse_State `protobuf:"varint,1,opt,name=state,proto3,enum=jobservice.JobServiceResponse_State" json:"state,omitempty"`
	// For failed jobs, this will contain a reason why the job failed
	Error string `protobuf:"bytes,2,opt,name=error,proto3" json:"error,omitempty"`
}

func (m *JobServiceResponse) Reset()         { *m = JobServiceResponse{} }
func (m *JobServiceResponse) String() string { return proto.CompactTextString(m) }
func (*JobServiceResponse) ProtoMessage()    {}
func (*JobServiceResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_acaf6279d0169157, []int{2}
}
func (m *JobServiceResponse) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JobServiceResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JobServiceResponse.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *JobServiceResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobServiceResponse.Merge(m, src)
}
func (m *JobServiceResponse) XXX_Size() int {
	return m.Size()
}
func (m *JobServiceResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_JobServiceResponse.DiscardUnknown(m)
}

var xxx_messageInfo_JobServiceResponse proto.InternalMessageInfo

func (m *JobServiceResponse) GetState() JobServiceResponse_State {
	if m != nil {
		return m.State
	}
	return JobServiceResponse_SUBMITTED
}

func (m *JobServiceResponse) GetError() string {
	if m != nil {
		return m.Error
	}
	return ""
}

func init() {
	proto.RegisterEnum("jobservice.HealthCheckResponse_ServingStatus", HealthCheckResponse_ServingStatus_name, HealthCheckResponse_ServingStatus_value)
	proto.RegisterEnum("jobservice.JobServiceResponse_State", JobServiceResponse_State_name, JobServiceResponse_State_value)
	proto.RegisterType((*HealthCheckResponse)(nil), "jobservice.HealthCheckResponse")
	proto.RegisterType((*JobServiceRequest)(nil), "jobservice.JobServiceRequest")
	proto.RegisterType((*JobServiceResponse)(nil), "jobservice.JobServiceResponse")
}

func init() {
	proto.RegisterFile("pkg/api/jobservice/jobservice.proto", fileDescriptor_acaf6279d0169157)
}

var fileDescriptor_acaf6279d0169157 = []byte{
	// 574 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x53, 0xcd, 0x6e, 0xd3, 0x4c,
	0x14, 0x8d, 0xd3, 0x2f, 0xee, 0xd7, 0x5b, 0xda, 0x9a, 0x69, 0x55, 0x55, 0x45, 0x38, 0x28, 0xb0,
	0x00, 0x04, 0xb6, 0x54, 0x58, 0xb1, 0x73, 0x6c, 0xb7, 0xb8, 0xa4, 0xe3, 0xca, 0x49, 0x40, 0x82,
	0x85, 0x65, 0x27, 0x43, 0xe2, 0xb4, 0xc9, 0xb8, 0xf6, 0x18, 0x89, 0x77, 0x60, 0xc1, 0x13, 0x00,
	0x6b, 0x16, 0x3c, 0x07, 0xcb, 0x2e, 0x59, 0x45, 0x28, 0xd9, 0xe5, 0x29, 0xd0, 0x8c, 0x13, 0xc5,
	0x55, 0xf9, 0xd9, 0x79, 0xce, 0x3d, 0xf7, 0x9e, 0x33, 0x67, 0xae, 0xe1, 0x6e, 0x7c, 0xd6, 0xd3,
	0x83, 0x38, 0xd2, 0x07, 0x34, 0x4c, 0x49, 0xf2, 0x2e, 0xea, 0x90, 0xc2, 0xa7, 0x16, 0x27, 0x94,
	0x51, 0x04, 0x4b, 0x64, 0xff, 0x56, 0x8f, 0xd2, 0xde, 0x39, 0xd1, 0x45, 0x25, 0xcc, 0xde, 0xea,
	0x64, 0x18, 0xb3, 0xf7, 0x39, 0xb1, 0xf6, 0x4d, 0x82, 0xed, 0xe7, 0x24, 0x38, 0x67, 0x7d, 0xb3,
	0x4f, 0x3a, 0x67, 0x1e, 0x49, 0x63, 0x3a, 0x4a, 0x09, 0x7a, 0x03, 0x72, 0xca, 0x02, 0x96, 0xa5,
	0x7b, 0xd2, 0x1d, 0xe9, 0xfe, 0xe6, 0xc1, 0x63, 0xad, 0xa0, 0xf1, 0x9b, 0x06, 0xad, 0xc9, 0x6b,
	0xa3, 0x5e, 0x53, 0x34, 0xd5, 0x77, 0x66, 0xe3, 0xaa, 0x92, 0x0f, 0x78, 0x44, 0x87, 0x11, 0x13,
	0x92, 0xde, 0x7c, 0x64, 0xed, 0x19, 0x6c, 0x5c, 0xa1, 0xa3, 0x75, 0x58, 0x6d, 0xe3, 0x17, 0xd8,
	0x7d, 0x85, 0x95, 0x12, 0x3f, 0x34, 0x6d, 0xef, 0xa5, 0x83, 0x8f, 0x14, 0x09, 0x6d, 0xc1, 0x3a,
	0x76, 0x5b, 0xfe, 0x02, 0x28, 0xd7, 0xbe, 0x48, 0x70, 0xf3, 0x98, 0x86, 0xcd, 0xdc, 0x8a, 0x47,
	0x2e, 0x32, 0x92, 0x32, 0xf4, 0x10, 0xe4, 0x01, 0x0d, 0xfd, 0xa8, 0x2b, 0xec, 0xae, 0xd5, 0xb7,
	0x67, 0xe3, 0xea, 0xd6, 0x80, 0x86, 0x4e, 0xb7, 0x20, 0x5f, 0x11, 0x00, 0x7a, 0x0a, 0x3c, 0x1d,
	0x3f, 0x25, 0x8c, 0xf3, 0xcb, 0x82, 0xbf, 0x3b, 0x1b, 0x57, 0xd1, 0x80, 0x8f, 0x65, 0x57, 0x5a,
	0xfe, 0x5f, 0x60, 0xe8, 0x01, 0x54, 0x2e, 0x32, 0x92, 0x91, 0xbd, 0x95, 0xa5, 0x80, 0x00, 0x8a,
	0x02, 0x02, 0xa8, 0x7d, 0x2e, 0x03, 0x2a, 0x5a, 0x9c, 0x47, 0xea, 0x42, 0x85, 0xdf, 0x9f, 0xcc,
	0x13, 0xbd, 0x57, 0x4c, 0xf4, 0x3a, 0x5d, 0xe3, 0xd1, 0x90, 0x5c, 0x47, 0xb4, 0x15, 0x75, 0x04,
	0xc0, 0x2d, 0x91, 0x24, 0xa1, 0xc9, 0xfc, 0x0e, 0x82, 0x2a, 0x80, 0x22, 0x55, 0x00, 0xb5, 0x0f,
	0x12, 0x54, 0xc4, 0x40, 0xb4, 0x01, 0x6b, 0xcd, 0x76, 0xfd, 0xc4, 0x69, 0xb5, 0x6c, 0x4b, 0x29,
	0xa1, 0x6d, 0xd8, 0xb2, 0xda, 0xa7, 0x0d, 0xc7, 0x34, 0x5a, 0xb6, 0x7f, 0xe8, 0xb6, 0xb1, 0xa5,
	0x48, 0xfc, 0x05, 0xbc, 0x36, 0xc6, 0x22, 0x70, 0x04, 0x20, 0x1f, 0x1a, 0x4e, 0xc3, 0xb6, 0x94,
	0x95, 0xbc, 0xd9, 0x34, 0x6d, 0xdb, 0xb2, 0x2d, 0xe5, 0x3f, 0x7e, 0x34, 0x0d, 0x6c, 0xda, 0x0d,
	0x5e, 0xad, 0xa0, 0x1d, 0x50, 0x8e, 0xdd, 0xba, 0xef, 0x58, 0x3e, 0x7f, 0xb2, 0x7c, 0x98, 0x8c,
	0x10, 0x6c, 0x9a, 0x2e, 0xc6, 0xb6, 0xd9, 0x72, 0x5c, 0xec, 0xdb, 0x9e, 0xa7, 0xac, 0x1e, 0x7c,
	0x92, 0x00, 0x96, 0x57, 0x46, 0x27, 0x70, 0xe3, 0x88, 0x30, 0x0e, 0xe4, 0xeb, 0x70, 0xfb, 0x4f,
	0xd1, 0x88, 0xc7, 0xde, 0x57, 0xff, 0x9e, 0x1c, 0x32, 0x40, 0xce, 0x37, 0x14, 0xed, 0x6a, 0xf9,
	0xee, 0x6b, 0x8b, 0xdd, 0xd7, 0x6c, 0x9e, 0xca, 0x7e, 0xf5, 0x1f, 0xdb, 0x5c, 0x1f, 0x7d, 0x9f,
	0xa8, 0xd2, 0xe5, 0x44, 0x95, 0x7e, 0x4e, 0x54, 0xe9, 0xe3, 0x54, 0x2d, 0x5d, 0x4e, 0xd5, 0xd2,
	0x8f, 0xa9, 0x5a, 0x7a, 0x7d, 0xd0, 0x8b, 0x58, 0x3f, 0x0b, 0xb5, 0x0e, 0x1d, 0xea, 0x41, 0x32,
	0x0c, 0xba, 0x41, 0x9c, 0xd0, 0x01, 0xe9, 0xb0, 0xf9, 0x49, 0xbf, 0xfe, 0x7b, 0x7e, 0x2d, 0x57,
	0x0d, 0x51, 0x3b, 0xcd, 0x99, 0x9a, 0x43, 0x35, 0x23, 0x8e, 0x0a, 0xd6, 0x43, 0x59, 0x18, 0x7c,
	0xf2, 0x2b, 0x00, 0x00, 0xff, 0xff, 0x26, 0xf4, 0x4c, 0x08, 0xdd, 0x03, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// JobServiceClient is the client API for JobService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type JobServiceClient interface {
	GetJobStatus(ctx context.Context, in *JobServiceRequest, opts ...grpc.CallOption) (*JobServiceResponse, error)
	Health(ctx context.Context, in *types.Empty, opts ...grpc.CallOption) (*HealthCheckResponse, error)
}

type jobServiceClient struct {
	cc *grpc.ClientConn
}

func NewJobServiceClient(cc *grpc.ClientConn) JobServiceClient {
	return &jobServiceClient{cc}
}

func (c *jobServiceClient) GetJobStatus(ctx context.Context, in *JobServiceRequest, opts ...grpc.CallOption) (*JobServiceResponse, error) {
	out := new(JobServiceResponse)
	err := c.cc.Invoke(ctx, "/jobservice.JobService/GetJobStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *jobServiceClient) Health(ctx context.Context, in *types.Empty, opts ...grpc.CallOption) (*HealthCheckResponse, error) {
	out := new(HealthCheckResponse)
	err := c.cc.Invoke(ctx, "/jobservice.JobService/Health", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobServiceServer is the server API for JobService service.
type JobServiceServer interface {
	GetJobStatus(context.Context, *JobServiceRequest) (*JobServiceResponse, error)
	Health(context.Context, *types.Empty) (*HealthCheckResponse, error)
}

// UnimplementedJobServiceServer can be embedded to have forward compatible implementations.
type UnimplementedJobServiceServer struct {
}

func (*UnimplementedJobServiceServer) GetJobStatus(ctx context.Context, req *JobServiceRequest) (*JobServiceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobStatus not implemented")
}
func (*UnimplementedJobServiceServer) Health(ctx context.Context, req *types.Empty) (*HealthCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Health not implemented")
}

func RegisterJobServiceServer(s *grpc.Server, srv JobServiceServer) {
	s.RegisterService(&_JobService_serviceDesc, srv)
}

func _JobService_GetJobStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobServiceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).GetJobStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/jobservice.JobService/GetJobStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).GetJobStatus(ctx, req.(*JobServiceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _JobService_Health_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(JobServiceServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/jobservice.JobService/Health",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(JobServiceServer).Health(ctx, req.(*types.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

var _JobService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "jobservice.JobService",
	HandlerType: (*JobServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetJobStatus",
			Handler:    _JobService_GetJobStatus_Handler,
		},
		{
			MethodName: "Health",
			Handler:    _JobService_Health_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/api/jobservice/jobservice.proto",
}

func (m *HealthCheckResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *HealthCheckResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *HealthCheckResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Status != 0 {
		i = encodeVarintJobservice(dAtA, i, uint64(m.Status))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func (m *JobServiceRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JobServiceRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *JobServiceRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Queue) > 0 {
		i -= len(m.Queue)
		copy(dAtA[i:], m.Queue)
		i = encodeVarintJobservice(dAtA, i, uint64(len(m.Queue)))
		i--
		dAtA[i] = 0x1a
	}
	if len(m.JobSetId) > 0 {
		i -= len(m.JobSetId)
		copy(dAtA[i:], m.JobSetId)
		i = encodeVarintJobservice(dAtA, i, uint64(len(m.JobSetId)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.JobId) > 0 {
		i -= len(m.JobId)
		copy(dAtA[i:], m.JobId)
		i = encodeVarintJobservice(dAtA, i, uint64(len(m.JobId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *JobServiceResponse) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JobServiceResponse) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *JobServiceResponse) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Error) > 0 {
		i -= len(m.Error)
		copy(dAtA[i:], m.Error)
		i = encodeVarintJobservice(dAtA, i, uint64(len(m.Error)))
		i--
		dAtA[i] = 0x12
	}
	if m.State != 0 {
		i = encodeVarintJobservice(dAtA, i, uint64(m.State))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func encodeVarintJobservice(dAtA []byte, offset int, v uint64) int {
	offset -= sovJobservice(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *HealthCheckResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Status != 0 {
		n += 1 + sovJobservice(uint64(m.Status))
	}
	return n
}

func (m *JobServiceRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.JobId)
	if l > 0 {
		n += 1 + l + sovJobservice(uint64(l))
	}
	l = len(m.JobSetId)
	if l > 0 {
		n += 1 + l + sovJobservice(uint64(l))
	}
	l = len(m.Queue)
	if l > 0 {
		n += 1 + l + sovJobservice(uint64(l))
	}
	return n
}

func (m *JobServiceResponse) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.State != 0 {
		n += 1 + sovJobservice(uint64(m.State))
	}
	l = len(m.Error)
	if l > 0 {
		n += 1 + l + sovJobservice(uint64(l))
	}
	return n
}

func sovJobservice(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozJobservice(x uint64) (n int) {
	return sovJobservice(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *HealthCheckResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowJobservice
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
			return fmt.Errorf("proto: HealthCheckResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: HealthCheckResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Status", wireType)
			}
			m.Status = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJobservice
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Status |= HealthCheckResponse_ServingStatus(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipJobservice(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthJobservice
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
func (m *JobServiceRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowJobservice
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
			return fmt.Errorf("proto: JobServiceRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JobServiceRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field JobId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJobservice
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
				return ErrInvalidLengthJobservice
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthJobservice
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.JobId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field JobSetId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJobservice
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
				return ErrInvalidLengthJobservice
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthJobservice
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.JobSetId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Queue", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJobservice
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
				return ErrInvalidLengthJobservice
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthJobservice
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Queue = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipJobservice(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthJobservice
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
func (m *JobServiceResponse) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowJobservice
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
			return fmt.Errorf("proto: JobServiceResponse: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JobServiceResponse: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field State", wireType)
			}
			m.State = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJobservice
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.State |= JobServiceResponse_State(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Error", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowJobservice
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
				return ErrInvalidLengthJobservice
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthJobservice
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Error = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipJobservice(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthJobservice
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
func skipJobservice(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowJobservice
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
					return 0, ErrIntOverflowJobservice
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
					return 0, ErrIntOverflowJobservice
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
				return 0, ErrInvalidLengthJobservice
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupJobservice
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthJobservice
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthJobservice        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowJobservice          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupJobservice = fmt.Errorf("proto: unexpected end of group")
)
