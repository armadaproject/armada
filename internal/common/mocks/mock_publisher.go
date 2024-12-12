// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/armadaproject/armada/internal/common/pulsarutils (interfaces: Publisher)
//
// Generated by this command:
//
//	mockgen -destination=./mock_publisher.go -package=mocks github.com/armadaproject/armada/internal/common/pulsarutils Publisher
//

// Package mocks is a generated GoMock package.
package mocks

import (
	reflect "reflect"

	armadacontext "github.com/armadaproject/armada/internal/common/armadacontext"
	utils "github.com/armadaproject/armada/internal/common/ingest/utils"
	gomock "go.uber.org/mock/gomock"
)

// MockPublisher is a mock of Publisher interface.
type MockPublisher[T utils.ArmadaEvent] struct {
	ctrl     *gomock.Controller
	recorder *MockPublisherMockRecorder[T]
	isgomock struct{}
}

// MockPublisherMockRecorder is the mock recorder for MockPublisher.
type MockPublisherMockRecorder[T utils.ArmadaEvent] struct {
	mock *MockPublisher[T]
}

// NewMockPublisher creates a new mock instance.
func NewMockPublisher[T utils.ArmadaEvent](ctrl *gomock.Controller) *MockPublisher[T] {
	mock := &MockPublisher[T]{ctrl: ctrl}
	mock.recorder = &MockPublisherMockRecorder[T]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPublisher[T]) EXPECT() *MockPublisherMockRecorder[T] {
	return m.recorder
}

// Close mocks base method.
func (m *MockPublisher[T]) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockPublisherMockRecorder[T]) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockPublisher[T])(nil).Close))
}

// PublishMessages mocks base method.
func (m *MockPublisher[T]) PublishMessages(ctx *armadacontext.Context, events ...T) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx}
	for _, a := range events {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PublishMessages", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// PublishMessages indicates an expected call of PublishMessages.
func (mr *MockPublisherMockRecorder[T]) PublishMessages(ctx any, events ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx}, events...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PublishMessages", reflect.TypeOf((*MockPublisher[T])(nil).PublishMessages), varargs...)
}
