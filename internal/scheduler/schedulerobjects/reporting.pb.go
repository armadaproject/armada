// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: internal/scheduler/schedulerobjects/reporting.proto

package schedulerobjects

import (
	context "context"
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
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

type SchedulingReport struct {
	Report string `protobuf:"bytes,1,opt,name=report,proto3" json:"report,omitempty"`
}

func (m *SchedulingReport) Reset()         { *m = SchedulingReport{} }
func (m *SchedulingReport) String() string { return proto.CompactTextString(m) }
func (*SchedulingReport) ProtoMessage()    {}
func (*SchedulingReport) Descriptor() ([]byte, []int) {
	return fileDescriptor_131a439a3ff6540b, []int{0}
}
func (m *SchedulingReport) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SchedulingReport) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SchedulingReport.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SchedulingReport) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SchedulingReport.Merge(m, src)
}
func (m *SchedulingReport) XXX_Size() int {
	return m.Size()
}
func (m *SchedulingReport) XXX_DiscardUnknown() {
	xxx_messageInfo_SchedulingReport.DiscardUnknown(m)
}

var xxx_messageInfo_SchedulingReport proto.InternalMessageInfo

func (m *SchedulingReport) GetReport() string {
	if m != nil {
		return m.Report
	}
	return ""
}

type Queue struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *Queue) Reset()         { *m = Queue{} }
func (m *Queue) String() string { return proto.CompactTextString(m) }
func (*Queue) ProtoMessage()    {}
func (*Queue) Descriptor() ([]byte, []int) {
	return fileDescriptor_131a439a3ff6540b, []int{1}
}
func (m *Queue) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Queue) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Queue.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Queue) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Queue.Merge(m, src)
}
func (m *Queue) XXX_Size() int {
	return m.Size()
}
func (m *Queue) XXX_DiscardUnknown() {
	xxx_messageInfo_Queue.DiscardUnknown(m)
}

var xxx_messageInfo_Queue proto.InternalMessageInfo

func (m *Queue) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

type QueueReport struct {
	Report string `protobuf:"bytes,1,opt,name=report,proto3" json:"report,omitempty"`
}

func (m *QueueReport) Reset()         { *m = QueueReport{} }
func (m *QueueReport) String() string { return proto.CompactTextString(m) }
func (*QueueReport) ProtoMessage()    {}
func (*QueueReport) Descriptor() ([]byte, []int) {
	return fileDescriptor_131a439a3ff6540b, []int{2}
}
func (m *QueueReport) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *QueueReport) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_QueueReport.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *QueueReport) XXX_Merge(src proto.Message) {
	xxx_messageInfo_QueueReport.Merge(m, src)
}
func (m *QueueReport) XXX_Size() int {
	return m.Size()
}
func (m *QueueReport) XXX_DiscardUnknown() {
	xxx_messageInfo_QueueReport.DiscardUnknown(m)
}

var xxx_messageInfo_QueueReport proto.InternalMessageInfo

func (m *QueueReport) GetReport() string {
	if m != nil {
		return m.Report
	}
	return ""
}

type JobId struct {
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (m *JobId) Reset()         { *m = JobId{} }
func (m *JobId) String() string { return proto.CompactTextString(m) }
func (*JobId) ProtoMessage()    {}
func (*JobId) Descriptor() ([]byte, []int) {
	return fileDescriptor_131a439a3ff6540b, []int{3}
}
func (m *JobId) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JobId) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JobId.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *JobId) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobId.Merge(m, src)
}
func (m *JobId) XXX_Size() int {
	return m.Size()
}
func (m *JobId) XXX_DiscardUnknown() {
	xxx_messageInfo_JobId.DiscardUnknown(m)
}

var xxx_messageInfo_JobId proto.InternalMessageInfo

func (m *JobId) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

type JobReport struct {
	Report string `protobuf:"bytes,1,opt,name=report,proto3" json:"report,omitempty"`
}

func (m *JobReport) Reset()         { *m = JobReport{} }
func (m *JobReport) String() string { return proto.CompactTextString(m) }
func (*JobReport) ProtoMessage()    {}
func (*JobReport) Descriptor() ([]byte, []int) {
	return fileDescriptor_131a439a3ff6540b, []int{4}
}
func (m *JobReport) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *JobReport) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_JobReport.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *JobReport) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobReport.Merge(m, src)
}
func (m *JobReport) XXX_Size() int {
	return m.Size()
}
func (m *JobReport) XXX_DiscardUnknown() {
	xxx_messageInfo_JobReport.DiscardUnknown(m)
}

var xxx_messageInfo_JobReport proto.InternalMessageInfo

func (m *JobReport) GetReport() string {
	if m != nil {
		return m.Report
	}
	return ""
}

func init() {
	proto.RegisterType((*SchedulingReport)(nil), "schedulerobjects.SchedulingReport")
	proto.RegisterType((*Queue)(nil), "schedulerobjects.Queue")
	proto.RegisterType((*QueueReport)(nil), "schedulerobjects.QueueReport")
	proto.RegisterType((*JobId)(nil), "schedulerobjects.JobId")
	proto.RegisterType((*JobReport)(nil), "schedulerobjects.JobReport")
}

func init() {
	proto.RegisterFile("internal/scheduler/schedulerobjects/reporting.proto", fileDescriptor_131a439a3ff6540b)
}

var fileDescriptor_131a439a3ff6540b = []byte{
	// 356 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x92, 0xc1, 0x6a, 0xea, 0x40,
	0x14, 0x86, 0x8d, 0x5c, 0x05, 0xcf, 0x15, 0x09, 0x73, 0x2f, 0xf7, 0x96, 0x48, 0xa3, 0x64, 0x51,
	0x5a, 0x90, 0x19, 0xa8, 0xab, 0xd2, 0x4d, 0xb1, 0x2d, 0xb6, 0xee, 0xd4, 0x5d, 0xa1, 0x8b, 0xc4,
	0x9c, 0xc6, 0x29, 0x26, 0x13, 0xc6, 0xc9, 0xa2, 0x6f, 0xd1, 0xc7, 0xea, 0xd2, 0x65, 0x57, 0x52,
	0x74, 0xe7, 0x0b, 0x74, 0x5b, 0x9c, 0x44, 0x14, 0xb5, 0x50, 0xdc, 0x65, 0xf2, 0x9f, 0xef, 0x3f,
	0xe4, 0xcb, 0x40, 0x93, 0x47, 0x0a, 0x65, 0xe4, 0x8e, 0xd8, 0x78, 0x30, 0x44, 0x3f, 0x19, 0xa1,
	0x5c, 0x3f, 0x09, 0xef, 0x19, 0x07, 0x6a, 0xcc, 0x24, 0xc6, 0x42, 0x2a, 0x1e, 0x05, 0x34, 0x96,
	0x42, 0x09, 0x62, 0x6e, 0x4f, 0x58, 0xd5, 0x40, 0x88, 0x60, 0x84, 0x4c, 0xe7, 0x5e, 0xf2, 0xc4,
	0x30, 0x8c, 0xd5, 0x4b, 0x3a, 0xee, 0x5c, 0x81, 0xd9, 0x4f, 0x01, 0x1e, 0x05, 0x3d, 0xdd, 0x45,
	0x1a, 0x50, 0x4c, 0x5b, 0x8f, 0x8c, 0xba, 0x71, 0x5a, 0x6a, 0xfd, 0x5d, 0x4c, 0x6b, 0x66, 0xfa,
	0xa6, 0x21, 0x42, 0xae, 0x34, 0xdf, 0xcb, 0x66, 0x1c, 0x06, 0x85, 0x6e, 0x82, 0x09, 0x92, 0x13,
	0xf8, 0x15, 0xb9, 0x21, 0x66, 0x10, 0x59, 0x4c, 0x6b, 0x95, 0xe5, 0x79, 0x03, 0xd1, 0xb9, 0x73,
	0x09, 0xbf, 0x35, 0x70, 0xd0, 0xb6, 0x33, 0x28, 0x74, 0x84, 0x77, 0xef, 0x93, 0x3a, 0xe4, 0xb9,
	0x9f, 0x21, 0xe6, 0x62, 0x5a, 0x2b, 0x73, 0x7f, 0x63, 0x3c, 0xcf, 0x7d, 0xe7, 0x02, 0x4a, 0x1d,
	0xe1, 0x1d, 0xb2, 0xe5, 0xfc, 0xd3, 0x00, 0xd2, 0x5f, 0x79, 0xec, 0xad, 0x0c, 0x93, 0x2e, 0xfc,
	0x69, 0xa3, 0xda, 0xf1, 0xf5, 0x8f, 0xa6, 0x86, 0xe9, 0xca, 0x30, 0xbd, 0x5d, 0xb6, 0x59, 0x0e,
	0xdd, 0xfe, 0x17, 0x74, 0x87, 0xbd, 0x83, 0x4a, 0x1b, 0xd5, 0xa6, 0x8f, 0xff, 0xbb, 0x94, 0x8e,
	0xad, 0xe3, 0x6f, 0x82, 0x8c, 0xbb, 0x81, 0x72, 0x1b, 0xd5, 0xfa, 0x8b, 0xf7, 0xf4, 0x68, 0x73,
	0x56, 0x75, 0x6f, 0x90, 0x52, 0xad, 0xc7, 0xb7, 0x99, 0x6d, 0x4c, 0x66, 0xb6, 0xf1, 0x31, 0xb3,
	0x8d, 0xd7, 0xb9, 0x9d, 0x9b, 0xcc, 0xed, 0xdc, 0xfb, 0xdc, 0xce, 0x3d, 0x5c, 0x07, 0x5c, 0x0d,
	0x13, 0x8f, 0x0e, 0x44, 0xc8, 0x5c, 0x19, 0xba, 0xbe, 0x1b, 0x4b, 0xb1, 0xc4, 0xb3, 0x13, 0xfb,
	0xc1, 0x6d, 0xf5, 0x8a, 0x5a, 0x51, 0xf3, 0x2b, 0x00, 0x00, 0xff, 0xff, 0xcd, 0xc1, 0x68, 0xdb,
	0xdb, 0x02, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// SchedulerReportingClient is the client API for SchedulerReporting service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SchedulerReportingClient interface {
	// Return the most recent scheduling report for each executor.
	GetSchedulingReport(ctx context.Context, in *types.Empty, opts ...grpc.CallOption) (*SchedulingReport, error)
	// Return the most recent report scheduling for each executor for the given queue.
	GetQueueReport(ctx context.Context, in *Queue, opts ...grpc.CallOption) (*QueueReport, error)
	// Return the most recent scheduling report for each executor for the given job.
	GetJobReport(ctx context.Context, in *JobId, opts ...grpc.CallOption) (*JobReport, error)
}

type schedulerReportingClient struct {
	cc *grpc.ClientConn
}

func NewSchedulerReportingClient(cc *grpc.ClientConn) SchedulerReportingClient {
	return &schedulerReportingClient{cc}
}

func (c *schedulerReportingClient) GetSchedulingReport(ctx context.Context, in *types.Empty, opts ...grpc.CallOption) (*SchedulingReport, error) {
	out := new(SchedulingReport)
	err := c.cc.Invoke(ctx, "/schedulerobjects.SchedulerReporting/GetSchedulingReport", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerReportingClient) GetQueueReport(ctx context.Context, in *Queue, opts ...grpc.CallOption) (*QueueReport, error) {
	out := new(QueueReport)
	err := c.cc.Invoke(ctx, "/schedulerobjects.SchedulerReporting/GetQueueReport", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *schedulerReportingClient) GetJobReport(ctx context.Context, in *JobId, opts ...grpc.CallOption) (*JobReport, error) {
	out := new(JobReport)
	err := c.cc.Invoke(ctx, "/schedulerobjects.SchedulerReporting/GetJobReport", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SchedulerReportingServer is the server API for SchedulerReporting service.
type SchedulerReportingServer interface {
	// Return the most recent scheduling report for each executor.
	GetSchedulingReport(context.Context, *types.Empty) (*SchedulingReport, error)
	// Return the most recent report scheduling for each executor for the given queue.
	GetQueueReport(context.Context, *Queue) (*QueueReport, error)
	// Return the most recent scheduling report for each executor for the given job.
	GetJobReport(context.Context, *JobId) (*JobReport, error)
}

// UnimplementedSchedulerReportingServer can be embedded to have forward compatible implementations.
type UnimplementedSchedulerReportingServer struct {
}

func (*UnimplementedSchedulerReportingServer) GetSchedulingReport(ctx context.Context, req *types.Empty) (*SchedulingReport, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetSchedulingReport not implemented")
}
func (*UnimplementedSchedulerReportingServer) GetQueueReport(ctx context.Context, req *Queue) (*QueueReport, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetQueueReport not implemented")
}
func (*UnimplementedSchedulerReportingServer) GetJobReport(ctx context.Context, req *JobId) (*JobReport, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobReport not implemented")
}

func RegisterSchedulerReportingServer(s *grpc.Server, srv SchedulerReportingServer) {
	s.RegisterService(&_SchedulerReporting_serviceDesc, srv)
}

func _SchedulerReporting_GetSchedulingReport_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(types.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerReportingServer).GetSchedulingReport(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/schedulerobjects.SchedulerReporting/GetSchedulingReport",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerReportingServer).GetSchedulingReport(ctx, req.(*types.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerReporting_GetQueueReport_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Queue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerReportingServer).GetQueueReport(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/schedulerobjects.SchedulerReporting/GetQueueReport",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerReportingServer).GetQueueReport(ctx, req.(*Queue))
	}
	return interceptor(ctx, in, info, handler)
}

func _SchedulerReporting_GetJobReport_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JobId)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SchedulerReportingServer).GetJobReport(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/schedulerobjects.SchedulerReporting/GetJobReport",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SchedulerReportingServer).GetJobReport(ctx, req.(*JobId))
	}
	return interceptor(ctx, in, info, handler)
}

var _SchedulerReporting_serviceDesc = grpc.ServiceDesc{
	ServiceName: "schedulerobjects.SchedulerReporting",
	HandlerType: (*SchedulerReportingServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetSchedulingReport",
			Handler:    _SchedulerReporting_GetSchedulingReport_Handler,
		},
		{
			MethodName: "GetQueueReport",
			Handler:    _SchedulerReporting_GetQueueReport_Handler,
		},
		{
			MethodName: "GetJobReport",
			Handler:    _SchedulerReporting_GetJobReport_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal/scheduler/schedulerobjects/reporting.proto",
}

func (m *SchedulingReport) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SchedulingReport) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SchedulingReport) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Report) > 0 {
		i -= len(m.Report)
		copy(dAtA[i:], m.Report)
		i = encodeVarintReporting(dAtA, i, uint64(len(m.Report)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Queue) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Queue) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Queue) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintReporting(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *QueueReport) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *QueueReport) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *QueueReport) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Report) > 0 {
		i -= len(m.Report)
		copy(dAtA[i:], m.Report)
		i = encodeVarintReporting(dAtA, i, uint64(len(m.Report)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *JobId) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JobId) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *JobId) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Id) > 0 {
		i -= len(m.Id)
		copy(dAtA[i:], m.Id)
		i = encodeVarintReporting(dAtA, i, uint64(len(m.Id)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *JobReport) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *JobReport) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *JobReport) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Report) > 0 {
		i -= len(m.Report)
		copy(dAtA[i:], m.Report)
		i = encodeVarintReporting(dAtA, i, uint64(len(m.Report)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintReporting(dAtA []byte, offset int, v uint64) int {
	offset -= sovReporting(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *SchedulingReport) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Report)
	if l > 0 {
		n += 1 + l + sovReporting(uint64(l))
	}
	return n
}

func (m *Queue) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovReporting(uint64(l))
	}
	return n
}

func (m *QueueReport) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Report)
	if l > 0 {
		n += 1 + l + sovReporting(uint64(l))
	}
	return n
}

func (m *JobId) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Id)
	if l > 0 {
		n += 1 + l + sovReporting(uint64(l))
	}
	return n
}

func (m *JobReport) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Report)
	if l > 0 {
		n += 1 + l + sovReporting(uint64(l))
	}
	return n
}

func sovReporting(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozReporting(x uint64) (n int) {
	return sovReporting(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *SchedulingReport) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowReporting
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
			return fmt.Errorf("proto: SchedulingReport: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SchedulingReport: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Report", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowReporting
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
				return ErrInvalidLengthReporting
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthReporting
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Report = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipReporting(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthReporting
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
func (m *Queue) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowReporting
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
			return fmt.Errorf("proto: Queue: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Queue: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowReporting
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
				return ErrInvalidLengthReporting
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthReporting
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipReporting(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthReporting
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
func (m *QueueReport) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowReporting
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
			return fmt.Errorf("proto: QueueReport: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: QueueReport: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Report", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowReporting
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
				return ErrInvalidLengthReporting
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthReporting
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Report = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipReporting(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthReporting
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
func (m *JobId) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowReporting
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
			return fmt.Errorf("proto: JobId: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JobId: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowReporting
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
				return ErrInvalidLengthReporting
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthReporting
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Id = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipReporting(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthReporting
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
func (m *JobReport) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowReporting
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
			return fmt.Errorf("proto: JobReport: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: JobReport: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Report", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowReporting
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
				return ErrInvalidLengthReporting
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthReporting
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Report = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipReporting(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthReporting
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
func skipReporting(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowReporting
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
					return 0, ErrIntOverflowReporting
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
					return 0, ErrIntOverflowReporting
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
				return 0, ErrInvalidLengthReporting
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupReporting
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthReporting
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthReporting        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowReporting          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupReporting = fmt.Errorf("proto: unexpected end of group")
)
