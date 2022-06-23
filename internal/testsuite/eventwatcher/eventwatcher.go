// Utility for watching events.
package eventwatcher

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
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
		return fmt.Sprintf("expected event of type %T, but got %+v", err.expected.Events, err.actual.Events)
	}
	return fmt.Sprintf("expected event of type %T, but got %+v; %s", err.expected.Events, err.actual.Events, err.message)
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
			return errors.Errorf("did not receive all events for at least one job")
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
	case *api.EventMessage_Cancelled:
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
					return errors.Errorf("received multiple terminal events for job %s", e.JobId)
				}
				exitedByJobId[e.JobId] = true
				if _, ok := jobIds[e.JobId]; ok {
					numRemaining--
				}
				numActive--
			} else if e := msg.GetFailed(); e != nil {
				if _, ok := exitedByJobId[e.JobId]; ok {
					return errors.Errorf("received multiple terminal events for job %s", e.JobId)
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

// WithCancelOnFailed returns a context that is cancelled if a job fails,
// or if the parent is cancelled.
func ErrorOnFailed(parent context.Context, C chan *api.EventMessage) error {
	for {
		select {
		case <-parent.Done():
			return parent.Err()
		case msg := <-C:
			if failedEvent := msg.GetFailed(); failedEvent != nil {
				err := errors.Errorf("job failed")
				return errors.WithMessagef(err, "%v", failedEvent)
			}
		}
	}
}

// GetFromIngresses listens for ingressInfo messages and tries to download from each ingress.
// Returns false if any download fails.
func GetFromIngresses(parent context.Context, C chan *api.EventMessage) error {
	g, ctx := errgroup.WithContext(parent)
	for {
		select {
		case <-parent.Done():
			return g.Wait()
		case <-ctx.Done(): // errgroup cancelled
			return g.Wait()
		case msg := <-C:
			if ingressInfo := msg.GetIngressInfo(); ingressInfo != nil {
				for _, host := range ingressInfo.IngressAddresses {
					ctxWithTimeout, _ := context.WithTimeout(ctx, 10*time.Second)
					host := host
					g.Go(func() error { return getFromIngress(ctxWithTimeout, host) })
				}
			}
		}
	}
}

func getFromIngress(ctx context.Context, host string) error {
	ingressUrl := os.Getenv("ARMADA_EXECUTOR_INGRESS_URL")
	if ingressUrl == "" {
		ingressUrl = "http://" + host
	}

	// The ingress info messages can't convey which port ingress are handled on (only the url).
	// (The assumption is that ingress is always handled on port 80.)
	// Here, we override this with an environment varibel so we can test against local Kind clusters,
	// which may handle ingress on an arbitrary port.
	ingressPort := os.Getenv("ARMADA_EXECUTOR_INGRESS_PORT")
	if ingressPort == "" {
		ingressPort = "80"
	}

	// Make a get request to test that the ingress works.
	// This assumes that whatever the ingress points to responds.
	httpClient := &http.Client{}
	httpReq, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s:%s/", ingressUrl, ingressPort), http.NoBody)
	if err != nil {
		return err
	}
	httpReq.Host = host

	// It may take a moment before ingresses are registered with the ingress controller.
	// so we retry until success or until ctx is cancelled.
	var requestErr error
	for {
		select {
		case <-ctx.Done():
			// When cancelled, return the most recent request err,
			// or if there have been no errors (context cancelled before the first attempt)
			// return the context error.
			if requestErr == nil {
				return ctx.Err()
			}
			return requestErr
		default:
			httpRes, err := httpClient.Do(httpReq)
			if err != nil {
				requestErr = err
				break
			}
			httpRes.Body.Close()

			if httpRes.StatusCode != 200 {
				requestErr = errors.Errorf("GET request failed for host %s: status code %d", host, httpRes.StatusCode)
				break
			}

			return nil
		}
	}
}
