// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pkg/metricevents/events.proto

package metricevents

import (
	encoding_binary "encoding/binary"
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
	resource "k8s.io/apimachinery/pkg/api/resource"
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

type Event struct {
	Created *types.Timestamp `protobuf:"bytes,1,opt,name=created,proto3" json:"created,omitempty"`
	// Types that are valid to be assigned to Event:
	//
	//	*Event_CycleMetrics
	Event isEvent_Event `protobuf_oneof:"event"`
}

func (m *Event) Reset()         { *m = Event{} }
func (m *Event) String() string { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()    {}
func (*Event) Descriptor() ([]byte, []int) {
	return fileDescriptor_932dcac363ef0bc9, []int{0}
}
func (m *Event) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Event) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Event.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Event) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Event.Merge(m, src)
}
func (m *Event) XXX_Size() int {
	return m.Size()
}
func (m *Event) XXX_DiscardUnknown() {
	xxx_messageInfo_Event.DiscardUnknown(m)
}

var xxx_messageInfo_Event proto.InternalMessageInfo

type isEvent_Event interface {
	isEvent_Event()
	MarshalTo([]byte) (int, error)
	Size() int
}

type Event_CycleMetrics struct {
	CycleMetrics *CycleMetrics `protobuf:"bytes,2,opt,name=cycle_metrics,json=cycleMetrics,proto3,oneof" json:"cycleMetrics,omitempty"`
}

func (*Event_CycleMetrics) isEvent_Event() {}

func (m *Event) GetEvent() isEvent_Event {
	if m != nil {
		return m.Event
	}
	return nil
}

func (m *Event) GetCreated() *types.Timestamp {
	if m != nil {
		return m.Created
	}
	return nil
}

func (m *Event) GetCycleMetrics() *CycleMetrics {
	if x, ok := m.GetEvent().(*Event_CycleMetrics); ok {
		return x.CycleMetrics
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Event) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Event_CycleMetrics)(nil),
	}
}

type CycleMetrics struct {
	// Pool that these metrics pertain to
	Pool string `protobuf:"bytes,1,opt,name=pool,proto3" json:"pool,omitempty"`
	// Fraction of the pool allocated per queue
	ActualShare map[string]float64 `protobuf:"bytes,2,rep,name=actual_share,json=actualShare,proto3" json:"actualShare,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
	// Fraction of the pool demanded per queue. May be greater than 1
	Demand map[string]float64 `protobuf:"bytes,3,rep,name=demand,proto3" json:"demand,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
	// Fraction of the pool demanded per queue, limited by scheduling constraints. May be greater than 1
	ConstrainedDemand map[string]float64 `protobuf:"bytes,4,rep,name=constrained_demand,json=constrainedDemand,proto3" json:"constrainedDemand,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"fixed64,2,opt,name=value,proto3"`
	// Allocatable resources
	AllocatableResources map[string]*resource.Quantity `protobuf:"bytes,5,rep,name=allocatable_resources,json=allocatableResources,proto3" json:"allocatableResources,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (m *CycleMetrics) Reset()         { *m = CycleMetrics{} }
func (m *CycleMetrics) String() string { return proto.CompactTextString(m) }
func (*CycleMetrics) ProtoMessage()    {}
func (*CycleMetrics) Descriptor() ([]byte, []int) {
	return fileDescriptor_932dcac363ef0bc9, []int{1}
}
func (m *CycleMetrics) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *CycleMetrics) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_CycleMetrics.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *CycleMetrics) XXX_Merge(src proto.Message) {
	xxx_messageInfo_CycleMetrics.Merge(m, src)
}
func (m *CycleMetrics) XXX_Size() int {
	return m.Size()
}
func (m *CycleMetrics) XXX_DiscardUnknown() {
	xxx_messageInfo_CycleMetrics.DiscardUnknown(m)
}

var xxx_messageInfo_CycleMetrics proto.InternalMessageInfo

func (m *CycleMetrics) GetPool() string {
	if m != nil {
		return m.Pool
	}
	return ""
}

func (m *CycleMetrics) GetActualShare() map[string]float64 {
	if m != nil {
		return m.ActualShare
	}
	return nil
}

func (m *CycleMetrics) GetDemand() map[string]float64 {
	if m != nil {
		return m.Demand
	}
	return nil
}

func (m *CycleMetrics) GetConstrainedDemand() map[string]float64 {
	if m != nil {
		return m.ConstrainedDemand
	}
	return nil
}

func (m *CycleMetrics) GetAllocatableResources() map[string]*resource.Quantity {
	if m != nil {
		return m.AllocatableResources
	}
	return nil
}

func init() {
	proto.RegisterType((*Event)(nil), "controlplaneevents.Event")
	proto.RegisterType((*CycleMetrics)(nil), "controlplaneevents.CycleMetrics")
	proto.RegisterMapType((map[string]float64)(nil), "controlplaneevents.CycleMetrics.ActualShareEntry")
	proto.RegisterMapType((map[string]*resource.Quantity)(nil), "controlplaneevents.CycleMetrics.AllocatableResourcesEntry")
	proto.RegisterMapType((map[string]float64)(nil), "controlplaneevents.CycleMetrics.ConstrainedDemandEntry")
	proto.RegisterMapType((map[string]float64)(nil), "controlplaneevents.CycleMetrics.DemandEntry")
}

func init() { proto.RegisterFile("pkg/metricevents/events.proto", fileDescriptor_932dcac363ef0bc9) }

var fileDescriptor_932dcac363ef0bc9 = []byte{
	// 602 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xb4, 0x94, 0xc1, 0x6e, 0xd3, 0x4c,
	0x10, 0xc7, 0xe3, 0xb6, 0x69, 0xf5, 0x6d, 0xda, 0x8f, 0x76, 0x69, 0xaa, 0xd4, 0x08, 0x3b, 0x0a,
	0x12, 0x2a, 0x52, 0xb5, 0x86, 0x80, 0x44, 0xd5, 0x1b, 0x2e, 0x95, 0x10, 0x88, 0x03, 0x81, 0x0b,
	0xbd, 0x44, 0x9b, 0xcd, 0xd6, 0x31, 0xb1, 0xbd, 0xd6, 0x7a, 0x5d, 0xc9, 0x4f, 0xc0, 0x15, 0x89,
	0x07, 0xe0, 0x5d, 0x38, 0x71, 0xec, 0x91, 0x93, 0x85, 0x92, 0x9b, 0x1f, 0x02, 0x21, 0xef, 0xc6,
	0xca, 0x2a, 0x0d, 0x0a, 0x97, 0x9e, 0x2c, 0xcf, 0x7f, 0xfe, 0xf3, 0x9b, 0xdd, 0x1d, 0x0d, 0xb8,
	0x1f, 0x8f, 0x3d, 0x27, 0xa4, 0x82, 0xfb, 0x84, 0x5e, 0xd1, 0x48, 0x24, 0x8e, 0xfa, 0xa0, 0x98,
	0x33, 0xc1, 0x20, 0x24, 0x2c, 0x12, 0x9c, 0x05, 0x71, 0x80, 0x23, 0xaa, 0x14, 0xd3, 0xf6, 0x18,
	0xf3, 0x02, 0xea, 0xc8, 0x8c, 0x41, 0x7a, 0xe9, 0x08, 0x3f, 0xa4, 0x89, 0xc0, 0x61, 0xac, 0x4c,
	0xe6, 0xb3, 0xf1, 0x49, 0x82, 0x7c, 0xe6, 0xe0, 0xd8, 0x0f, 0x31, 0x19, 0xf9, 0x11, 0xe5, 0x99,
	0x53, 0x72, 0x70, 0xec, 0x3b, 0x9c, 0x26, 0x2c, 0xe5, 0x84, 0x3a, 0x1e, 0x8d, 0x28, 0xc7, 0x82,
	0x0e, 0x95, 0xab, 0xf3, 0xdd, 0x00, 0xf5, 0xf3, 0x92, 0x00, 0xdf, 0x80, 0x2d, 0xc2, 0x69, 0x29,
	0xb5, 0x8c, 0xb6, 0x71, 0xd4, 0xe8, 0x9a, 0x48, 0x21, 0x51, 0x85, 0x44, 0x1f, 0x2a, 0xa4, 0xdb,
	0x2c, 0x72, 0x7b, 0x6f, 0x96, 0x7e, 0xcc, 0x42, 0x5f, 0xd0, 0x30, 0x16, 0x59, 0xaf, 0xaa, 0x00,
	0x09, 0xd8, 0x21, 0x19, 0x09, 0x68, 0x5f, 0x1d, 0x32, 0x69, 0xad, 0xc9, 0x92, 0x6d, 0x74, 0xf3,
	0x64, 0xe8, 0xac, 0x4c, 0x7c, 0xab, 0xf2, 0x5c, 0xb3, 0xc8, 0xed, 0x03, 0xa2, 0x45, 0xe6, 0xd5,
	0x5f, 0xd5, 0x7a, 0xdb, 0xba, 0xe2, 0x6e, 0x81, 0xba, 0x2c, 0xd1, 0xf9, 0xbd, 0x05, 0xb6, 0xf5,
	0x2a, 0xf0, 0x21, 0xd8, 0x88, 0x19, 0x0b, 0xe4, 0x41, 0xfe, 0x73, 0x61, 0x91, 0xdb, 0xff, 0x97,
	0xff, 0x5a, 0xa7, 0x52, 0x87, 0x0c, 0x6c, 0x63, 0x22, 0x52, 0x1c, 0xf4, 0x93, 0x11, 0xe6, 0xb4,
	0xb5, 0xd6, 0x5e, 0x3f, 0x6a, 0x74, 0x9f, 0xac, 0xea, 0x12, 0xbd, 0x90, 0xa6, 0xf7, 0xa5, 0xe7,
	0x3c, 0x12, 0x3c, 0x73, 0x0f, 0x8b, 0xdc, 0x6e, 0xe2, 0x79, 0x54, 0x23, 0x35, 0xb4, 0x30, 0xbc,
	0x00, 0x9b, 0x43, 0x1a, 0xe2, 0x68, 0xd8, 0x5a, 0x97, 0xa8, 0xe3, 0x95, 0xa8, 0x97, 0x32, 0x5d,
	0x51, 0xf6, 0x8b, 0xdc, 0xde, 0x55, 0x7e, 0x0d, 0x30, 0xab, 0x08, 0x3f, 0x1b, 0xa0, 0x1c, 0x9c,
	0x44, 0x70, 0xec, 0x47, 0x74, 0xd8, 0x9f, 0x81, 0x36, 0x24, 0xe8, 0xf9, 0x4a, 0xd0, 0xd9, 0xdc,
	0xaa, 0x33, 0xed, 0x22, 0xb7, 0xef, 0x91, 0x45, 0x4d, 0xc3, 0xef, 0xdd, 0x10, 0xe1, 0x57, 0x03,
	0x34, 0x71, 0x10, 0x30, 0x82, 0x05, 0x1e, 0x04, 0xb4, 0x5f, 0x4d, 0x5f, 0xd2, 0xaa, 0xcb, 0x66,
	0x4e, 0x57, 0x5f, 0xf0, 0xdc, 0xdd, 0xab, 0xcc, 0xaa, 0x9f, 0x4e, 0x91, 0xdb, 0x16, 0x5e, 0x22,
	0x6b, 0x2d, 0xed, 0x2f, 0xd3, 0xcd, 0x4b, 0xb0, 0xbb, 0xf8, 0x6e, 0xf0, 0x01, 0x58, 0x1f, 0xd3,
	0x6c, 0x36, 0x27, 0x7b, 0x45, 0x6e, 0xef, 0x8c, 0x69, 0xa6, 0x55, 0x2a, 0x55, 0xf8, 0x08, 0xd4,
	0xaf, 0x70, 0x90, 0x52, 0x39, 0xc4, 0x86, 0x7b, 0xb7, 0xc8, 0xed, 0x3b, 0x32, 0xa0, 0x25, 0xaa,
	0x8c, 0xd3, 0xb5, 0x13, 0xc3, 0xc4, 0xa0, 0xa1, 0x5d, 0xe0, 0xad, 0x20, 0x02, 0x70, 0xb0, 0xfc,
	0xb9, 0x6e, 0x85, 0xf6, 0xcd, 0x00, 0x87, 0x7f, 0x7d, 0x90, 0x7f, 0x23, 0x7e, 0xd4, 0x89, 0x8d,
	0x2e, 0x42, 0x6a, 0x59, 0x21, 0x7d, 0x59, 0xa1, 0x78, 0xec, 0x95, 0x01, 0x54, 0x8d, 0x0b, 0x7a,
	0x97, 0xe2, 0x48, 0xf8, 0x22, 0x5b, 0xd5, 0xa1, 0xfb, 0xfa, 0xc7, 0xc4, 0x32, 0xae, 0x27, 0x96,
	0xf1, 0x6b, 0x62, 0x19, 0x5f, 0xa6, 0x56, 0xed, 0x7a, 0x6a, 0xd5, 0x7e, 0x4e, 0xad, 0xda, 0xc5,
	0x63, 0xcf, 0x17, 0xa3, 0x74, 0x80, 0x08, 0x0b, 0x1d, 0xcc, 0x43, 0x3c, 0xc4, 0x31, 0x67, 0x9f,
	0x28, 0x11, 0xb3, 0x3f, 0x67, 0x71, 0x13, 0x0f, 0x36, 0xe5, 0xba, 0x7b, 0xfa, 0x27, 0x00, 0x00,
	0xff, 0xff, 0xeb, 0x5d, 0x62, 0x84, 0xa4, 0x05, 0x00, 0x00,
}

func (m *Event) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Event) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Event) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Event != nil {
		{
			size := m.Event.Size()
			i -= size
			if _, err := m.Event.MarshalTo(dAtA[i:]); err != nil {
				return 0, err
			}
		}
	}
	if m.Created != nil {
		{
			size, err := m.Created.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintEvents(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *Event_CycleMetrics) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Event_CycleMetrics) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.CycleMetrics != nil {
		{
			size, err := m.CycleMetrics.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintEvents(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	return len(dAtA) - i, nil
}
func (m *CycleMetrics) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *CycleMetrics) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *CycleMetrics) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.AllocatableResources) > 0 {
		for k := range m.AllocatableResources {
			v := m.AllocatableResources[k]
			baseI := i
			if v != nil {
				{
					size, err := v.MarshalToSizedBuffer(dAtA[:i])
					if err != nil {
						return 0, err
					}
					i -= size
					i = encodeVarintEvents(dAtA, i, uint64(size))
				}
				i--
				dAtA[i] = 0x12
			}
			i -= len(k)
			copy(dAtA[i:], k)
			i = encodeVarintEvents(dAtA, i, uint64(len(k)))
			i--
			dAtA[i] = 0xa
			i = encodeVarintEvents(dAtA, i, uint64(baseI-i))
			i--
			dAtA[i] = 0x2a
		}
	}
	if len(m.ConstrainedDemand) > 0 {
		for k := range m.ConstrainedDemand {
			v := m.ConstrainedDemand[k]
			baseI := i
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(v))))
			i--
			dAtA[i] = 0x11
			i -= len(k)
			copy(dAtA[i:], k)
			i = encodeVarintEvents(dAtA, i, uint64(len(k)))
			i--
			dAtA[i] = 0xa
			i = encodeVarintEvents(dAtA, i, uint64(baseI-i))
			i--
			dAtA[i] = 0x22
		}
	}
	if len(m.Demand) > 0 {
		for k := range m.Demand {
			v := m.Demand[k]
			baseI := i
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(v))))
			i--
			dAtA[i] = 0x11
			i -= len(k)
			copy(dAtA[i:], k)
			i = encodeVarintEvents(dAtA, i, uint64(len(k)))
			i--
			dAtA[i] = 0xa
			i = encodeVarintEvents(dAtA, i, uint64(baseI-i))
			i--
			dAtA[i] = 0x1a
		}
	}
	if len(m.ActualShare) > 0 {
		for k := range m.ActualShare {
			v := m.ActualShare[k]
			baseI := i
			i -= 8
			encoding_binary.LittleEndian.PutUint64(dAtA[i:], uint64(math.Float64bits(float64(v))))
			i--
			dAtA[i] = 0x11
			i -= len(k)
			copy(dAtA[i:], k)
			i = encodeVarintEvents(dAtA, i, uint64(len(k)))
			i--
			dAtA[i] = 0xa
			i = encodeVarintEvents(dAtA, i, uint64(baseI-i))
			i--
			dAtA[i] = 0x12
		}
	}
	if len(m.Pool) > 0 {
		i -= len(m.Pool)
		copy(dAtA[i:], m.Pool)
		i = encodeVarintEvents(dAtA, i, uint64(len(m.Pool)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintEvents(dAtA []byte, offset int, v uint64) int {
	offset -= sovEvents(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Event) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Created != nil {
		l = m.Created.Size()
		n += 1 + l + sovEvents(uint64(l))
	}
	if m.Event != nil {
		n += m.Event.Size()
	}
	return n
}

func (m *Event_CycleMetrics) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.CycleMetrics != nil {
		l = m.CycleMetrics.Size()
		n += 1 + l + sovEvents(uint64(l))
	}
	return n
}
func (m *CycleMetrics) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Pool)
	if l > 0 {
		n += 1 + l + sovEvents(uint64(l))
	}
	if len(m.ActualShare) > 0 {
		for k, v := range m.ActualShare {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovEvents(uint64(len(k))) + 1 + 8
			n += mapEntrySize + 1 + sovEvents(uint64(mapEntrySize))
		}
	}
	if len(m.Demand) > 0 {
		for k, v := range m.Demand {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovEvents(uint64(len(k))) + 1 + 8
			n += mapEntrySize + 1 + sovEvents(uint64(mapEntrySize))
		}
	}
	if len(m.ConstrainedDemand) > 0 {
		for k, v := range m.ConstrainedDemand {
			_ = k
			_ = v
			mapEntrySize := 1 + len(k) + sovEvents(uint64(len(k))) + 1 + 8
			n += mapEntrySize + 1 + sovEvents(uint64(mapEntrySize))
		}
	}
	if len(m.AllocatableResources) > 0 {
		for k, v := range m.AllocatableResources {
			_ = k
			_ = v
			l = 0
			if v != nil {
				l = v.Size()
				l += 1 + sovEvents(uint64(l))
			}
			mapEntrySize := 1 + len(k) + sovEvents(uint64(len(k))) + l
			n += mapEntrySize + 1 + sovEvents(uint64(mapEntrySize))
		}
	}
	return n
}

func sovEvents(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozEvents(x uint64) (n int) {
	return sovEvents(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *Event) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowEvents
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
			return fmt.Errorf("proto: Event: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: Event: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Created", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Created == nil {
				m.Created = &types.Timestamp{}
			}
			if err := m.Created.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field CycleMetrics", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			v := &CycleMetrics{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Event = &Event_CycleMetrics{v}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipEvents(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthEvents
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
func (m *CycleMetrics) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowEvents
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
			return fmt.Errorf("proto: CycleMetrics: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: CycleMetrics: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Pool", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Pool = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ActualShare", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.ActualShare == nil {
				m.ActualShare = make(map[string]float64)
			}
			var mapkey string
			var mapvalue float64
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowEvents
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
							return ErrIntOverflowEvents
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
						return ErrInvalidLengthEvents
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey < 0 {
						return ErrInvalidLengthEvents
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
					skippy, err := skipEvents(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if (skippy < 0) || (iNdEx+skippy) < 0 {
						return ErrInvalidLengthEvents
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.ActualShare[mapkey] = mapvalue
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Demand", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Demand == nil {
				m.Demand = make(map[string]float64)
			}
			var mapkey string
			var mapvalue float64
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowEvents
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
							return ErrIntOverflowEvents
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
						return ErrInvalidLengthEvents
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey < 0 {
						return ErrInvalidLengthEvents
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
					skippy, err := skipEvents(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if (skippy < 0) || (iNdEx+skippy) < 0 {
						return ErrInvalidLengthEvents
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.Demand[mapkey] = mapvalue
			iNdEx = postIndex
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ConstrainedDemand", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.ConstrainedDemand == nil {
				m.ConstrainedDemand = make(map[string]float64)
			}
			var mapkey string
			var mapvalue float64
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowEvents
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
							return ErrIntOverflowEvents
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
						return ErrInvalidLengthEvents
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey < 0 {
						return ErrInvalidLengthEvents
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
					skippy, err := skipEvents(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if (skippy < 0) || (iNdEx+skippy) < 0 {
						return ErrInvalidLengthEvents
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.ConstrainedDemand[mapkey] = mapvalue
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field AllocatableResources", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
				return ErrInvalidLengthEvents
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthEvents
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.AllocatableResources == nil {
				m.AllocatableResources = make(map[string]*resource.Quantity)
			}
			var mapkey string
			var mapvalue *resource.Quantity
			for iNdEx < postIndex {
				entryPreIndex := iNdEx
				var wire uint64
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return ErrIntOverflowEvents
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
							return ErrIntOverflowEvents
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
						return ErrInvalidLengthEvents
					}
					postStringIndexmapkey := iNdEx + intStringLenmapkey
					if postStringIndexmapkey < 0 {
						return ErrInvalidLengthEvents
					}
					if postStringIndexmapkey > l {
						return io.ErrUnexpectedEOF
					}
					mapkey = string(dAtA[iNdEx:postStringIndexmapkey])
					iNdEx = postStringIndexmapkey
				} else if fieldNum == 2 {
					var mapmsglen int
					for shift := uint(0); ; shift += 7 {
						if shift >= 64 {
							return ErrIntOverflowEvents
						}
						if iNdEx >= l {
							return io.ErrUnexpectedEOF
						}
						b := dAtA[iNdEx]
						iNdEx++
						mapmsglen |= int(b&0x7F) << shift
						if b < 0x80 {
							break
						}
					}
					if mapmsglen < 0 {
						return ErrInvalidLengthEvents
					}
					postmsgIndex := iNdEx + mapmsglen
					if postmsgIndex < 0 {
						return ErrInvalidLengthEvents
					}
					if postmsgIndex > l {
						return io.ErrUnexpectedEOF
					}
					mapvalue = &resource.Quantity{}
					if err := mapvalue.Unmarshal(dAtA[iNdEx:postmsgIndex]); err != nil {
						return err
					}
					iNdEx = postmsgIndex
				} else {
					iNdEx = entryPreIndex
					skippy, err := skipEvents(dAtA[iNdEx:])
					if err != nil {
						return err
					}
					if (skippy < 0) || (iNdEx+skippy) < 0 {
						return ErrInvalidLengthEvents
					}
					if (iNdEx + skippy) > postIndex {
						return io.ErrUnexpectedEOF
					}
					iNdEx += skippy
				}
			}
			m.AllocatableResources[mapkey] = mapvalue
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipEvents(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthEvents
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
func skipEvents(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowEvents
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
					return 0, ErrIntOverflowEvents
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
					return 0, ErrIntOverflowEvents
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
				return 0, ErrInvalidLengthEvents
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupEvents
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthEvents
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthEvents        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowEvents          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupEvents = fmt.Errorf("proto: unexpected end of group")
)
