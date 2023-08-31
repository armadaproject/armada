// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/armadaproject/armada/internal/scheduler/database (interfaces: ExecutorRepository,QueueRepository,JobRepository)

// Package schedulermocks is a generated GoMock package.
package schedulermocks

import (
	reflect "reflect"
	time "time"

	context "github.com/armadaproject/armada/internal/common/context"
	database "github.com/armadaproject/armada/internal/scheduler/database"
	schedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	armadaevents "github.com/armadaproject/armada/pkg/armadaevents"
	gomock "github.com/golang/mock/gomock"
	uuid "github.com/google/uuid"
)

// MockExecutorRepository is a mock of ExecutorRepository interface.
type MockExecutorRepository struct {
	ctrl     *gomock.Controller
	recorder *MockExecutorRepositoryMockRecorder
}

// MockExecutorRepositoryMockRecorder is the mock recorder for MockExecutorRepository.
type MockExecutorRepositoryMockRecorder struct {
	mock *MockExecutorRepository
}

// NewMockExecutorRepository creates a new mock instance.
func NewMockExecutorRepository(ctrl *gomock.Controller) *MockExecutorRepository {
	mock := &MockExecutorRepository{ctrl: ctrl}
	mock.recorder = &MockExecutorRepositoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockExecutorRepository) EXPECT() *MockExecutorRepositoryMockRecorder {
	return m.recorder
}

// GetExecutors mocks base method.
func (m *MockExecutorRepository) GetExecutors(arg0 *context.ArmadaContext) ([]*schedulerobjects.Executor, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetExecutors", arg0)
	ret0, _ := ret[0].([]*schedulerobjects.Executor)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetExecutors indicates an expected call of GetExecutors.
func (mr *MockExecutorRepositoryMockRecorder) GetExecutors(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetExecutors", reflect.TypeOf((*MockExecutorRepository)(nil).GetExecutors), arg0)
}

// GetLastUpdateTimes mocks base method.
func (m *MockExecutorRepository) GetLastUpdateTimes(arg0 *context.ArmadaContext) (map[string]time.Time, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLastUpdateTimes", arg0)
	ret0, _ := ret[0].(map[string]time.Time)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLastUpdateTimes indicates an expected call of GetLastUpdateTimes.
func (mr *MockExecutorRepositoryMockRecorder) GetLastUpdateTimes(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLastUpdateTimes", reflect.TypeOf((*MockExecutorRepository)(nil).GetLastUpdateTimes), arg0)
}

// StoreExecutor mocks base method.
func (m *MockExecutorRepository) StoreExecutor(arg0 *context.ArmadaContext, arg1 *schedulerobjects.Executor) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreExecutor", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// StoreExecutor indicates an expected call of StoreExecutor.
func (mr *MockExecutorRepositoryMockRecorder) StoreExecutor(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreExecutor", reflect.TypeOf((*MockExecutorRepository)(nil).StoreExecutor), arg0, arg1)
}

// MockQueueRepository is a mock of QueueRepository interface.
type MockQueueRepository struct {
	ctrl     *gomock.Controller
	recorder *MockQueueRepositoryMockRecorder
}

// MockQueueRepositoryMockRecorder is the mock recorder for MockQueueRepository.
type MockQueueRepositoryMockRecorder struct {
	mock *MockQueueRepository
}

// NewMockQueueRepository creates a new mock instance.
func NewMockQueueRepository(ctrl *gomock.Controller) *MockQueueRepository {
	mock := &MockQueueRepository{ctrl: ctrl}
	mock.recorder = &MockQueueRepositoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockQueueRepository) EXPECT() *MockQueueRepositoryMockRecorder {
	return m.recorder
}

// GetAllQueues mocks base method.
func (m *MockQueueRepository) GetAllQueues() ([]*database.Queue, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllQueues")
	ret0, _ := ret[0].([]*database.Queue)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllQueues indicates an expected call of GetAllQueues.
func (mr *MockQueueRepositoryMockRecorder) GetAllQueues() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllQueues", reflect.TypeOf((*MockQueueRepository)(nil).GetAllQueues))
}

// MockJobRepository is a mock of JobRepository interface.
type MockJobRepository struct {
	ctrl     *gomock.Controller
	recorder *MockJobRepositoryMockRecorder
}

// MockJobRepositoryMockRecorder is the mock recorder for MockJobRepository.
type MockJobRepositoryMockRecorder struct {
	mock *MockJobRepository
}

// NewMockJobRepository creates a new mock instance.
func NewMockJobRepository(ctrl *gomock.Controller) *MockJobRepository {
	mock := &MockJobRepository{ctrl: ctrl}
	mock.recorder = &MockJobRepositoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockJobRepository) EXPECT() *MockJobRepositoryMockRecorder {
	return m.recorder
}

// CountReceivedPartitions mocks base method.
func (m *MockJobRepository) CountReceivedPartitions(arg0 *context.ArmadaContext, arg1 uuid.UUID) (uint32, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CountReceivedPartitions", arg0, arg1)
	ret0, _ := ret[0].(uint32)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CountReceivedPartitions indicates an expected call of CountReceivedPartitions.
func (mr *MockJobRepositoryMockRecorder) CountReceivedPartitions(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CountReceivedPartitions", reflect.TypeOf((*MockJobRepository)(nil).CountReceivedPartitions), arg0, arg1)
}

// FetchJobRunErrors mocks base method.
func (m *MockJobRepository) FetchJobRunErrors(arg0 *context.ArmadaContext, arg1 []uuid.UUID) (map[uuid.UUID]*armadaevents.Error, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchJobRunErrors", arg0, arg1)
	ret0, _ := ret[0].(map[uuid.UUID]*armadaevents.Error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchJobRunErrors indicates an expected call of FetchJobRunErrors.
func (mr *MockJobRepositoryMockRecorder) FetchJobRunErrors(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchJobRunErrors", reflect.TypeOf((*MockJobRepository)(nil).FetchJobRunErrors), arg0, arg1)
}

// FetchJobRunLeases mocks base method.
func (m *MockJobRepository) FetchJobRunLeases(arg0 *context.ArmadaContext, arg1 string, arg2 uint, arg3 []uuid.UUID) ([]*database.JobRunLease, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchJobRunLeases", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]*database.JobRunLease)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchJobRunLeases indicates an expected call of FetchJobRunLeases.
func (mr *MockJobRepositoryMockRecorder) FetchJobRunLeases(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchJobRunLeases", reflect.TypeOf((*MockJobRepository)(nil).FetchJobRunLeases), arg0, arg1, arg2, arg3)
}

// FetchJobUpdates mocks base method.
func (m *MockJobRepository) FetchJobUpdates(arg0 *context.ArmadaContext, arg1, arg2 int64) ([]database.Job, []database.Run, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FetchJobUpdates", arg0, arg1, arg2)
	ret0, _ := ret[0].([]database.Job)
	ret1, _ := ret[1].([]database.Run)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// FetchJobUpdates indicates an expected call of FetchJobUpdates.
func (mr *MockJobRepositoryMockRecorder) FetchJobUpdates(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchJobUpdates", reflect.TypeOf((*MockJobRepository)(nil).FetchJobUpdates), arg0, arg1, arg2)
}

// FindInactiveRuns mocks base method.
func (m *MockJobRepository) FindInactiveRuns(arg0 *context.ArmadaContext, arg1 []uuid.UUID) ([]uuid.UUID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FindInactiveRuns", arg0, arg1)
	ret0, _ := ret[0].([]uuid.UUID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FindInactiveRuns indicates an expected call of FindInactiveRuns.
func (mr *MockJobRepositoryMockRecorder) FindInactiveRuns(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FindInactiveRuns", reflect.TypeOf((*MockJobRepository)(nil).FindInactiveRuns), arg0, arg1)
}
