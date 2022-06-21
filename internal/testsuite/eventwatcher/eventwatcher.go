// Utility for watching events.
package eventwatcher

import (
	"context"
	"fmt"
	"reflect"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/pkg/errors"
)

// EventWatcher is a service for watching for events and forwarding those on C.
// It connects to a server using ApiConnectionDetails and subscribes to events (queue, jobSetName).
type EventWatcher struct {
	Queue                string
	JobSetName           string
	ApiConnectionDetails *client.ApiConnectionDetails
	C                    chan *api.EventMessage
}

func New(queue string, jobSetName string, apiConnectionDetails *client.ApiConnectionDetails) *EventWatcher {
	return &EventWatcher{
		Queue:                queue,
		JobSetName:           jobSetName,
		ApiConnectionDetails: apiConnectionDetails,
		C:                    make(chan *api.EventMessage),
	}
}

// Run starts the service.
func (srv *EventWatcher) Run(ctx context.Context) error {
	fmt.Println("Watcher started")
	defer fmt.Println("Watcher stopped")
	return client.WithEventClient(srv.ApiConnectionDetails, func(c api.EventClient) error {
		stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
			Id:    srv.JobSetName,
			Queue: srv.Queue,
			Watch: true,
		})
		if err != nil {
			return err
		}
		for {
			msg, err := stream.Recv()
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case srv.C <- msg.Message:
			}
		}
	})
}

// ErrUnexpectedEvent indicates the wrong event type was received.
type ErrUnexpectedEvent struct {
	expected *api.EventMessage
	actual   *api.EventMessage
	message  string
}

func (err *ErrUnexpectedEvent) Error() string {
	if err.message == "" {
		return fmt.Sprintf("expected event of type %T, but got %T", err.expected, err.actual)
	}
	return fmt.Sprintf("expected event of type %T, but got %T; %s", err.expected, err.actual, err.message)
}

// AssertEvents compares the events received for each job with the expected events.
func AssertEvents(ctx context.Context, c chan *api.EventMessage, jobIds map[string]bool, expected []*api.EventMessage) error {
	if len(expected) == 0 {
		return nil
	}

	// Track which events have been seen for each job
	indexByJobId := make(map[string]int)
	numDone := 0

	// Receive events until all expected events have been received
	// or an unexpected event has been received.
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("did not receive all events for at least one job")
		case actual := <-c:
			actualJobId := api.JobIdFromApiEvent(actual)
			_, ok := jobIds[actualJobId]
			if !ok {
				break // Unrecognised job id
			}

			i := indexByJobId[actualJobId]
			if i < len(expected) && reflect.TypeOf(actual.Events) == reflect.TypeOf(expected[i].Events) {
				i++
				indexByJobId[actualJobId] = i
			}
			if i == len(expected) {
				numDone++
				if numDone == len(jobIds) {
					return nil // We got all the expected events.
				}
			}

			// Return an error if the job has exited without us seeing all expected events.
			if isTerminalEvent(actual) && i < len(expected) {
				return &ErrUnexpectedEvent{
					expected: expected[i],
					actual:   actual,
				}
			}
		}
	}
}

func isTerminalEvent(msg *api.EventMessage) bool {
	switch msg.Events.(type) {
	case *api.EventMessage_Failed:
		return true
	case *api.EventMessage_Succeeded:
		return true
	}
	return false
}

// WithCancelOnNoActiveJobs returns a context that is cancelled when there are no active jobs,
// or if the parent is cancelled.
func ErrorOnNoActiveJobs(parent context.Context, C chan *api.EventMessage, jobIds map[string]bool) error {
	numActive := 0
	numRemaining := len(jobIds)
	exitedByJobId := make(map[string]bool)
	for {
		select {
		case <-parent.Done():
			return nil
		case msg := <-C:
			if e := msg.GetSubmitted(); e != nil {
				numActive++
			} else if e := msg.GetSucceeded(); e != nil {
				if _, ok := exitedByJobId[e.JobId]; ok {
					return fmt.Errorf("received multiple terminal events for job %s", e.JobId)
				}
				exitedByJobId[e.JobId] = true
				if _, ok := jobIds[e.JobId]; ok {
					numRemaining--
				}
				numActive--
			} else if e := msg.GetFailed(); e != nil {
				if _, ok := exitedByJobId[e.JobId]; ok {
					return fmt.Errorf("received multiple terminal events for job %s", e.JobId)
				}
				exitedByJobId[e.JobId] = true
				if _, ok := jobIds[e.JobId]; ok {
					numRemaining--
				}
				numActive--
			}
			if numRemaining <= 0 {
				return errors.New("all jobs exited")
			}
		}
	}
}

// // WithCancelOnNoActiveJobs returns a context that is cancelled when there are no active jobs,
// // or if the parent is cancelled.
// func WithCancelOnNoActiveJobs(parent context.Context, C chan *api.EventMessage, jobIds map[string]bool) context.Context {
// 	ctx, cancel := context.WithCancel(parent)
// 	go func() {
// 		numActive := 0
// 		numRemaining := len(jobIds)
// 		exitedByJobId := make(map[string]bool)
// 		defer cancel()
// 		defer fmt.Println("WithCancelOnNoActiveJobs stopped")
// 		for {
// 			fmt.Println("WithCancelOnNoActiveJobs", numActive)
// 			select {
// 			case <-parent.Done():
// 				return
// 			case msg := <-C:
// 				if e := msg.GetSubmitted(); e != nil {
// 					numActive++
// 				} else if e := msg.GetSucceeded(); e != nil {
// 					if _, ok := exitedByJobId[e.jobId]; ok {
// 						fmt.Println("")
// 						return errors.New("received multiple terminal events for job %s", e.JobId)
// 					}
// 					numActive--
// 				} else if e := msg.GetFailed(); e != nil {
// 					numActive--
// 				}
// 				if numActive <= 0 {
// 					return
// 				}
// 			}
// 		}
// 	}()
// 	return ctx
// }

// WithCancelOnFailed returns a context that is cancelled if a job fails,
// or if the parent is cancelled.
func ErrorOnFailed(parent context.Context, C chan *api.EventMessage) error {
	defer fmt.Println("ErrorOnFailed stopped")
	for {
		select {
		case <-parent.Done():
			return parent.Err()
		case msg := <-C:
			if failedEvent := msg.GetFailed(); failedEvent != nil {
				err := fmt.Errorf("job failed")
				return errors.WithMessagef(err, "%v", failedEvent)
			}
		}
	}
}

type EventKey string

const FailedEventKey EventKey = "FailedEvent"

// WithCancelOnFailed returns a context that is cancelled if a job fails,
// or if the parent is cancelled.
func WithCancelOnFailed(parent context.Context, C chan *api.EventMessage) context.Context {
	ctx, cancel := context.WithCancel(parent)
	var failedEvent *api.JobFailedEvent
	ctx = context.WithValue(ctx, FailedEventKey, &failedEvent)
	go func() {
		defer cancel()
		defer fmt.Println("WithCancelOnFailed stopped")
		for {
			select {
			case <-parent.Done():
				return
			case msg := <-C:
				if failedEvent = msg.GetFailed(); failedEvent != nil {
					return
				}
			}
		}
	}()
	return ctx
}
