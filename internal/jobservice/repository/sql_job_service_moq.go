// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package repository

import (
	"sync"
)

// Ensure, that JobTableUpdaterMock does implement JobTableUpdater.
// If this is not the case, regenerate this file with moq.
var _ JobTableUpdater = &JobTableUpdaterMock{}

// JobTableUpdaterMock is a mock implementation of JobTableUpdater.
//
// 	func TestSomethingThatUsesJobTableUpdater(t *testing.T) {
//
// 		// make and configure a mocked JobTableUpdater
// 		mockedJobTableUpdater := &JobTableUpdaterMock{
// 			ClearSubscriptionErrorFunc: func(queue string, jobSet string)  {
// 				panic("mock out the ClearSubscriptionError method")
// 			},
// 			GetSubscriptionErrorFunc: func(queue string, jobSet string) string {
// 				panic("mock out the GetSubscriptionError method")
// 			},
// 			IsJobSetSubscribedFunc: func(queue string, jobSet string) bool {
// 				panic("mock out the IsJobSetSubscribed method")
// 			},
// 			SetSubscriptionErrorFunc: func(queue string, jobSet string, err string)  {
// 				panic("mock out the SetSubscriptionError method")
// 			},
// 			SubscribeJobSetFunc: func(queue string, jobSet string)  {
// 				panic("mock out the SubscribeJobSet method")
// 			},
// 			UpdateJobServiceDbFunc: func(jobStatus *JobStatus) error {
// 				panic("mock out the UpdateJobServiceDb method")
// 			},
// 		}
//
// 		// use mockedJobTableUpdater in code that requires JobTableUpdater
// 		// and then make assertions.
//
// 	}
type JobTableUpdaterMock struct {
	// ClearSubscriptionErrorFunc mocks the ClearSubscriptionError method.
	ClearSubscriptionErrorFunc func(queue string, jobSet string)

	// GetSubscriptionErrorFunc mocks the GetSubscriptionError method.
	GetSubscriptionErrorFunc func(queue string, jobSet string) string

	// IsJobSetSubscribedFunc mocks the IsJobSetSubscribed method.
	IsJobSetSubscribedFunc func(queue string, jobSet string) bool

	// SetSubscriptionErrorFunc mocks the SetSubscriptionError method.
	SetSubscriptionErrorFunc func(queue string, jobSet string, err string)

	// SubscribeJobSetFunc mocks the SubscribeJobSet method.
	SubscribeJobSetFunc func(queue string, jobSet string)

	// UpdateJobServiceDbFunc mocks the UpdateJobServiceDb method.
	UpdateJobServiceDbFunc func(jobStatus *JobStatus) error

	// calls tracks calls to the methods.
	calls struct {
		// ClearSubscriptionError holds details about calls to the ClearSubscriptionError method.
		ClearSubscriptionError []struct {
			// Queue is the queue argument value.
			Queue string
			// JobSet is the jobSet argument value.
			JobSet string
		}
		// GetSubscriptionError holds details about calls to the GetSubscriptionError method.
		GetSubscriptionError []struct {
			// Queue is the queue argument value.
			Queue string
			// JobSet is the jobSet argument value.
			JobSet string
		}
		// IsJobSetSubscribed holds details about calls to the IsJobSetSubscribed method.
		IsJobSetSubscribed []struct {
			// Queue is the queue argument value.
			Queue string
			// JobSet is the jobSet argument value.
			JobSet string
		}
		// SetSubscriptionError holds details about calls to the SetSubscriptionError method.
		SetSubscriptionError []struct {
			// Queue is the queue argument value.
			Queue string
			// JobSet is the jobSet argument value.
			JobSet string
			// Err is the err argument value.
			Err string
		}
		// SubscribeJobSet holds details about calls to the SubscribeJobSet method.
		SubscribeJobSet []struct {
			// Queue is the queue argument value.
			Queue string
			// JobSet is the jobSet argument value.
			JobSet string
		}
		// UpdateJobServiceDb holds details about calls to the UpdateJobServiceDb method.
		UpdateJobServiceDb []struct {
			// JobStatus is the jobStatus argument value.
			JobStatus *JobStatus
		}
	}
	lockClearSubscriptionError sync.RWMutex
	lockGetSubscriptionError   sync.RWMutex
	lockIsJobSetSubscribed     sync.RWMutex
	lockSetSubscriptionError   sync.RWMutex
	lockSubscribeJobSet        sync.RWMutex
	lockUpdateJobServiceDb     sync.RWMutex
}

// ClearSubscriptionError calls ClearSubscriptionErrorFunc.
func (mock *JobTableUpdaterMock) ClearSubscriptionError(queue string, jobSet string) {
	if mock.ClearSubscriptionErrorFunc == nil {
		panic("JobTableUpdaterMock.ClearSubscriptionErrorFunc: method is nil but JobTableUpdater.ClearSubscriptionError was just called")
	}
	callInfo := struct {
		Queue  string
		JobSet string
	}{
		Queue:  queue,
		JobSet: jobSet,
	}
	mock.lockClearSubscriptionError.Lock()
	mock.calls.ClearSubscriptionError = append(mock.calls.ClearSubscriptionError, callInfo)
	mock.lockClearSubscriptionError.Unlock()
	mock.ClearSubscriptionErrorFunc(queue, jobSet)
}

// ClearSubscriptionErrorCalls gets all the calls that were made to ClearSubscriptionError.
// Check the length with:
//     len(mockedJobTableUpdater.ClearSubscriptionErrorCalls())
func (mock *JobTableUpdaterMock) ClearSubscriptionErrorCalls() []struct {
	Queue  string
	JobSet string
} {
	var calls []struct {
		Queue  string
		JobSet string
	}
	mock.lockClearSubscriptionError.RLock()
	calls = mock.calls.ClearSubscriptionError
	mock.lockClearSubscriptionError.RUnlock()
	return calls
}

// GetSubscriptionError calls GetSubscriptionErrorFunc.
func (mock *JobTableUpdaterMock) GetSubscriptionError(queue string, jobSet string) string {
	if mock.GetSubscriptionErrorFunc == nil {
		panic("JobTableUpdaterMock.GetSubscriptionErrorFunc: method is nil but JobTableUpdater.GetSubscriptionError was just called")
	}
	callInfo := struct {
		Queue  string
		JobSet string
	}{
		Queue:  queue,
		JobSet: jobSet,
	}
	mock.lockGetSubscriptionError.Lock()
	mock.calls.GetSubscriptionError = append(mock.calls.GetSubscriptionError, callInfo)
	mock.lockGetSubscriptionError.Unlock()
	return mock.GetSubscriptionErrorFunc(queue, jobSet)
}

// GetSubscriptionErrorCalls gets all the calls that were made to GetSubscriptionError.
// Check the length with:
//     len(mockedJobTableUpdater.GetSubscriptionErrorCalls())
func (mock *JobTableUpdaterMock) GetSubscriptionErrorCalls() []struct {
	Queue  string
	JobSet string
} {
	var calls []struct {
		Queue  string
		JobSet string
	}
	mock.lockGetSubscriptionError.RLock()
	calls = mock.calls.GetSubscriptionError
	mock.lockGetSubscriptionError.RUnlock()
	return calls
}

// IsJobSetSubscribed calls IsJobSetSubscribedFunc.
func (mock *JobTableUpdaterMock) IsJobSetSubscribed(queue string, jobSet string) bool {
	if mock.IsJobSetSubscribedFunc == nil {
		panic("JobTableUpdaterMock.IsJobSetSubscribedFunc: method is nil but JobTableUpdater.IsJobSetSubscribed was just called")
	}
	callInfo := struct {
		Queue  string
		JobSet string
	}{
		Queue:  queue,
		JobSet: jobSet,
	}
	mock.lockIsJobSetSubscribed.Lock()
	mock.calls.IsJobSetSubscribed = append(mock.calls.IsJobSetSubscribed, callInfo)
	mock.lockIsJobSetSubscribed.Unlock()
	return mock.IsJobSetSubscribedFunc(queue, jobSet)
}

// IsJobSetSubscribedCalls gets all the calls that were made to IsJobSetSubscribed.
// Check the length with:
//     len(mockedJobTableUpdater.IsJobSetSubscribedCalls())
func (mock *JobTableUpdaterMock) IsJobSetSubscribedCalls() []struct {
	Queue  string
	JobSet string
} {
	var calls []struct {
		Queue  string
		JobSet string
	}
	mock.lockIsJobSetSubscribed.RLock()
	calls = mock.calls.IsJobSetSubscribed
	mock.lockIsJobSetSubscribed.RUnlock()
	return calls
}

// SetSubscriptionError calls SetSubscriptionErrorFunc.
func (mock *JobTableUpdaterMock) SetSubscriptionError(queue string, jobSet string, err string) {
	if mock.SetSubscriptionErrorFunc == nil {
		panic("JobTableUpdaterMock.SetSubscriptionErrorFunc: method is nil but JobTableUpdater.SetSubscriptionError was just called")
	}
	callInfo := struct {
		Queue  string
		JobSet string
		Err    string
	}{
		Queue:  queue,
		JobSet: jobSet,
		Err:    err,
	}
	mock.lockSetSubscriptionError.Lock()
	mock.calls.SetSubscriptionError = append(mock.calls.SetSubscriptionError, callInfo)
	mock.lockSetSubscriptionError.Unlock()
	mock.SetSubscriptionErrorFunc(queue, jobSet, err)
}

// SetSubscriptionErrorCalls gets all the calls that were made to SetSubscriptionError.
// Check the length with:
//     len(mockedJobTableUpdater.SetSubscriptionErrorCalls())
func (mock *JobTableUpdaterMock) SetSubscriptionErrorCalls() []struct {
	Queue  string
	JobSet string
	Err    string
} {
	var calls []struct {
		Queue  string
		JobSet string
		Err    string
	}
	mock.lockSetSubscriptionError.RLock()
	calls = mock.calls.SetSubscriptionError
	mock.lockSetSubscriptionError.RUnlock()
	return calls
}

// SubscribeJobSet calls SubscribeJobSetFunc.
func (mock *JobTableUpdaterMock) SubscribeJobSet(queue string, jobSet string) {
	if mock.SubscribeJobSetFunc == nil {
		panic("JobTableUpdaterMock.SubscribeJobSetFunc: method is nil but JobTableUpdater.SubscribeJobSet was just called")
	}
	callInfo := struct {
		Queue  string
		JobSet string
	}{
		Queue:  queue,
		JobSet: jobSet,
	}
	mock.lockSubscribeJobSet.Lock()
	mock.calls.SubscribeJobSet = append(mock.calls.SubscribeJobSet, callInfo)
	mock.lockSubscribeJobSet.Unlock()
	mock.SubscribeJobSetFunc(queue, jobSet)
}

// SubscribeJobSetCalls gets all the calls that were made to SubscribeJobSet.
// Check the length with:
//     len(mockedJobTableUpdater.SubscribeJobSetCalls())
func (mock *JobTableUpdaterMock) SubscribeJobSetCalls() []struct {
	Queue  string
	JobSet string
} {
	var calls []struct {
		Queue  string
		JobSet string
	}
	mock.lockSubscribeJobSet.RLock()
	calls = mock.calls.SubscribeJobSet
	mock.lockSubscribeJobSet.RUnlock()
	return calls
}

// UpdateJobServiceDb calls UpdateJobServiceDbFunc.
func (mock *JobTableUpdaterMock) UpdateJobServiceDb(jobStatus *JobStatus) error {
	if mock.UpdateJobServiceDbFunc == nil {
		panic("JobTableUpdaterMock.UpdateJobServiceDbFunc: method is nil but JobTableUpdater.UpdateJobServiceDb was just called")
	}
	callInfo := struct {
		JobStatus *JobStatus
	}{
		JobStatus: jobStatus,
	}
	mock.lockUpdateJobServiceDb.Lock()
	mock.calls.UpdateJobServiceDb = append(mock.calls.UpdateJobServiceDb, callInfo)
	mock.lockUpdateJobServiceDb.Unlock()
	return mock.UpdateJobServiceDbFunc(jobStatus)
}

// UpdateJobServiceDbCalls gets all the calls that were made to UpdateJobServiceDb.
// Check the length with:
//     len(mockedJobTableUpdater.UpdateJobServiceDbCalls())
func (mock *JobTableUpdaterMock) UpdateJobServiceDbCalls() []struct {
	JobStatus *JobStatus
} {
	var calls []struct {
		JobStatus *JobStatus
	}
	mock.lockUpdateJobServiceDb.RLock()
	calls = mock.calls.UpdateJobServiceDb
	mock.lockUpdateJobServiceDb.RUnlock()
	return calls
}
