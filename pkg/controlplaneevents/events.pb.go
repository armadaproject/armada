// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: pkg/controlplaneevents/events.proto

package controlplaneevents

import (
	fmt "fmt"
	io "io"
	math "math"
	math_bits "math/bits"

	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
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
	//	*Event_ExecutorSettingsUpsert
	//	*Event_ExecutorSettingsDelete
	Event isEvent_Event `protobuf_oneof:"event"`
}

func (m *Event) Reset()         { *m = Event{} }
func (m *Event) String() string { return proto.CompactTextString(m) }
func (*Event) ProtoMessage()    {}
func (*Event) Descriptor() ([]byte, []int) {
	return fileDescriptor_2ccee8bdbf348752, []int{0}
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

type Event_ExecutorSettingsUpsert struct {
	ExecutorSettingsUpsert *ExecutorSettingsUpsert `protobuf:"bytes,2,opt,name=executorSettingsUpsert,proto3,oneof" json:"executorSettingsUpsert,omitempty"`
}
type Event_ExecutorSettingsDelete struct {
	ExecutorSettingsDelete *ExecutorSettingsDelete `protobuf:"bytes,3,opt,name=executorSettingsDelete,proto3,oneof" json:"executorSettingsDelete,omitempty"`
}

func (*Event_ExecutorSettingsUpsert) isEvent_Event() {}
func (*Event_ExecutorSettingsDelete) isEvent_Event() {}

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

func (m *Event) GetExecutorSettingsUpsert() *ExecutorSettingsUpsert {
	if x, ok := m.GetEvent().(*Event_ExecutorSettingsUpsert); ok {
		return x.ExecutorSettingsUpsert
	}
	return nil
}

func (m *Event) GetExecutorSettingsDelete() *ExecutorSettingsDelete {
	if x, ok := m.GetEvent().(*Event_ExecutorSettingsDelete); ok {
		return x.ExecutorSettingsDelete
	}
	return nil
}

// XXX_OneofWrappers is for the internal use of the proto package.
func (*Event) XXX_OneofWrappers() []interface{} {
	return []interface{}{
		(*Event_ExecutorSettingsUpsert)(nil),
		(*Event_ExecutorSettingsDelete)(nil),
	}
}

type ExecutorSettingsUpsert struct {
	Name         string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Cordoned     bool   `protobuf:"varint,2,opt,name=cordoned,proto3" json:"cordoned,omitempty"`
	CordonReason string `protobuf:"bytes,3,opt,name=cordonReason,proto3" json:"cordonReason,omitempty"`
}

func (m *ExecutorSettingsUpsert) Reset()         { *m = ExecutorSettingsUpsert{} }
func (m *ExecutorSettingsUpsert) String() string { return proto.CompactTextString(m) }
func (*ExecutorSettingsUpsert) ProtoMessage()    {}
func (*ExecutorSettingsUpsert) Descriptor() ([]byte, []int) {
	return fileDescriptor_2ccee8bdbf348752, []int{1}
}
func (m *ExecutorSettingsUpsert) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ExecutorSettingsUpsert) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ExecutorSettingsUpsert.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ExecutorSettingsUpsert) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExecutorSettingsUpsert.Merge(m, src)
}
func (m *ExecutorSettingsUpsert) XXX_Size() int {
	return m.Size()
}
func (m *ExecutorSettingsUpsert) XXX_DiscardUnknown() {
	xxx_messageInfo_ExecutorSettingsUpsert.DiscardUnknown(m)
}

var xxx_messageInfo_ExecutorSettingsUpsert proto.InternalMessageInfo

func (m *ExecutorSettingsUpsert) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ExecutorSettingsUpsert) GetCordoned() bool {
	if m != nil {
		return m.Cordoned
	}
	return false
}

func (m *ExecutorSettingsUpsert) GetCordonReason() string {
	if m != nil {
		return m.CordonReason
	}
	return ""
}

type ExecutorSettingsDelete struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *ExecutorSettingsDelete) Reset()         { *m = ExecutorSettingsDelete{} }
func (m *ExecutorSettingsDelete) String() string { return proto.CompactTextString(m) }
func (*ExecutorSettingsDelete) ProtoMessage()    {}
func (*ExecutorSettingsDelete) Descriptor() ([]byte, []int) {
	return fileDescriptor_2ccee8bdbf348752, []int{2}
}
func (m *ExecutorSettingsDelete) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *ExecutorSettingsDelete) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_ExecutorSettingsDelete.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *ExecutorSettingsDelete) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ExecutorSettingsDelete.Merge(m, src)
}
func (m *ExecutorSettingsDelete) XXX_Size() int {
	return m.Size()
}
func (m *ExecutorSettingsDelete) XXX_DiscardUnknown() {
	xxx_messageInfo_ExecutorSettingsDelete.DiscardUnknown(m)
}

var xxx_messageInfo_ExecutorSettingsDelete proto.InternalMessageInfo

func (m *ExecutorSettingsDelete) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func init() {
	proto.RegisterType((*Event)(nil), "controlplaneevents.Event")
	proto.RegisterType((*ExecutorSettingsUpsert)(nil), "controlplaneevents.ExecutorSettingsUpsert")
	proto.RegisterType((*ExecutorSettingsDelete)(nil), "controlplaneevents.ExecutorSettingsDelete")
}

func init() {
	proto.RegisterFile("pkg/controlplaneevents/events.proto", fileDescriptor_2ccee8bdbf348752)
}

var fileDescriptor_2ccee8bdbf348752 = []byte{
	// 410 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x93, 0xc1, 0x6a, 0xdb, 0x40,
	0x10, 0x86, 0x25, 0xb7, 0xae, 0xed, 0x6d, 0x29, 0x74, 0xa1, 0xc2, 0xf8, 0x20, 0x19, 0xbb, 0x94,
	0x52, 0xca, 0x0a, 0x5c, 0xe8, 0xb1, 0xb4, 0x6a, 0x0d, 0x2d, 0xb9, 0x18, 0x27, 0xb9, 0xe4, 0xb6,
	0x96, 0x26, 0x8a, 0x12, 0x49, 0x2b, 0x56, 0x6b, 0x93, 0xbc, 0x42, 0x4e, 0x79, 0x8e, 0x9c, 0xf3,
	0x10, 0x39, 0xfa, 0x98, 0x93, 0x08, 0xf6, 0x4d, 0x2f, 0x90, 0x6b, 0xf0, 0xae, 0x95, 0x28, 0x58,
	0x09, 0x3e, 0x49, 0xbb, 0xf3, 0xfd, 0x33, 0xff, 0x8c, 0x46, 0xa8, 0x9f, 0x9c, 0xf8, 0xb6, 0xcb,
	0x62, 0xc1, 0x59, 0x98, 0x84, 0x34, 0x06, 0x98, 0x41, 0x2c, 0x52, 0x5b, 0x3d, 0x48, 0xc2, 0x99,
	0x60, 0x18, 0x6f, 0x02, 0x1d, 0xcb, 0x67, 0xcc, 0x0f, 0xc1, 0x96, 0xc4, 0x64, 0x7a, 0x68, 0x8b,
	0x20, 0x82, 0x54, 0xd0, 0x28, 0x51, 0xa2, 0xde, 0x5d, 0x0d, 0xd5, 0x87, 0x2b, 0x16, 0xef, 0xa0,
	0x86, 0xcb, 0x81, 0x0a, 0xf0, 0xda, 0x7a, 0x57, 0xff, 0xf2, 0x76, 0xd0, 0x21, 0x4a, 0x4c, 0x0a,
	0x31, 0xd9, 0x2b, 0xc4, 0xce, 0xc7, 0x3c, 0xb3, 0x3e, 0xac, 0xf1, 0x6f, 0x2c, 0x0a, 0x04, 0x44,
	0x89, 0x38, 0x1b, 0x17, 0x19, 0xf0, 0xb9, 0x8e, 0x0c, 0x38, 0x05, 0x77, 0x2a, 0x18, 0xdf, 0x05,
	0x21, 0x82, 0xd8, 0x4f, 0xf7, 0x93, 0x14, 0xb8, 0x68, 0xd7, 0x64, 0xf2, 0xaf, 0x64, 0xd3, 0x2d,
	0x19, 0x56, 0x2a, 0x9c, 0x4f, 0x79, 0x66, 0x75, 0xab, 0xb3, 0x3d, 0xd6, 0xfe, 0xa7, 0x8d, 0x9f,
	0xa9, 0x58, 0x69, 0xe6, 0x2f, 0x84, 0x20, 0xa0, 0xfd, 0x6a, 0x7b, 0x33, 0x4a, 0x51, 0x6d, 0x46,
	0xc5, 0x5e, 0x36, 0xb3, 0xd6, 0x37, 0x50, 0x5d, 0x16, 0xe8, 0x5d, 0xe9, 0xc8, 0xa8, 0x6e, 0x18,
	0x7f, 0x46, 0xaf, 0x63, 0x1a, 0x81, 0xfc, 0x0e, 0x2d, 0x07, 0xe7, 0x99, 0xf5, 0x7e, 0x75, 0x2e,
	0x0d, 0x5a, 0xc6, 0xf1, 0x00, 0x35, 0x5d, 0xc6, 0x3d, 0x16, 0x83, 0x27, 0xc7, 0xda, 0x74, 0x8c,
	0x3c, 0xb3, 0x70, 0x71, 0x57, 0xe2, 0x1f, 0x38, 0xfc, 0x13, 0xbd, 0x53, 0xef, 0x63, 0xa0, 0x29,
	0x8b, 0xe5, 0x04, 0x5a, 0x4e, 0x27, 0xcf, 0x2c, 0xa3, 0x7c, 0x5f, 0xd2, 0x3e, 0xe1, 0x7b, 0xbf,
	0x36, 0x5d, 0xab, 0xce, 0xb6, 0x75, 0xed, 0xcc, 0xae, 0x17, 0xa6, 0x3e, 0x5f, 0x98, 0xfa, 0xed,
	0xc2, 0xd4, 0x2f, 0x96, 0xa6, 0x36, 0x5f, 0x9a, 0xda, 0xcd, 0xd2, 0xd4, 0x0e, 0x7e, 0xf8, 0x81,
	0x38, 0x9a, 0x4e, 0x88, 0xcb, 0x22, 0x9b, 0xf2, 0x88, 0x7a, 0x34, 0xe1, 0xec, 0x18, 0x5c, 0xb1,
	0x3e, 0xd9, 0xd5, 0xbf, 0xc1, 0x65, 0xad, 0xff, 0x5b, 0xc6, 0x47, 0x8a, 0x26, 0xff, 0x19, 0xf9,
	0xa3, 0xa8, 0xd1, 0x8a, 0x92, 0xfb, 0x9d, 0x4e, 0xde, 0xc8, 0x3d, 0xfe, 0x7e, 0x1f, 0x00, 0x00,
	0xff, 0xff, 0x34, 0x77, 0x66, 0x52, 0x4d, 0x03, 0x00, 0x00,
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

func (m *Event_ExecutorSettingsUpsert) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Event_ExecutorSettingsUpsert) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.ExecutorSettingsUpsert != nil {
		{
			size, err := m.ExecutorSettingsUpsert.MarshalToSizedBuffer(dAtA[:i])
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
func (m *Event_ExecutorSettingsDelete) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Event_ExecutorSettingsDelete) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	if m.ExecutorSettingsDelete != nil {
		{
			size, err := m.ExecutorSettingsDelete.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintEvents(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x1a
	}
	return len(dAtA) - i, nil
}
func (m *ExecutorSettingsUpsert) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ExecutorSettingsUpsert) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ExecutorSettingsUpsert) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.CordonReason) > 0 {
		i -= len(m.CordonReason)
		copy(dAtA[i:], m.CordonReason)
		i = encodeVarintEvents(dAtA, i, uint64(len(m.CordonReason)))
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
		i = encodeVarintEvents(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *ExecutorSettingsDelete) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *ExecutorSettingsDelete) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *ExecutorSettingsDelete) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintEvents(dAtA, i, uint64(len(m.Name)))
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

func (m *Event_ExecutorSettingsUpsert) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.ExecutorSettingsUpsert != nil {
		l = m.ExecutorSettingsUpsert.Size()
		n += 1 + l + sovEvents(uint64(l))
	}
	return n
}
func (m *Event_ExecutorSettingsDelete) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.ExecutorSettingsDelete != nil {
		l = m.ExecutorSettingsDelete.Size()
		n += 1 + l + sovEvents(uint64(l))
	}
	return n
}
func (m *ExecutorSettingsUpsert) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovEvents(uint64(l))
	}
	if m.Cordoned {
		n += 2
	}
	l = len(m.CordonReason)
	if l > 0 {
		n += 1 + l + sovEvents(uint64(l))
	}
	return n
}

func (m *ExecutorSettingsDelete) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovEvents(uint64(l))
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
				return fmt.Errorf("proto: wrong wireType = %d for field ExecutorSettingsUpsert", wireType)
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
			v := &ExecutorSettingsUpsert{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Event = &Event_ExecutorSettingsUpsert{v}
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field ExecutorSettingsDelete", wireType)
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
			v := &ExecutorSettingsDelete{}
			if err := v.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			m.Event = &Event_ExecutorSettingsDelete{v}
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
func (m *ExecutorSettingsUpsert) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: ExecutorSettingsUpsert: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExecutorSettingsUpsert: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
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
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Cordoned", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowEvents
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
			m.CordonReason = string(dAtA[iNdEx:postIndex])
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
func (m *ExecutorSettingsDelete) Unmarshal(dAtA []byte) error {
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
			return fmt.Errorf("proto: ExecutorSettingsDelete: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: ExecutorSettingsDelete: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
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
			m.Name = string(dAtA[iNdEx:postIndex])
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
