// Utility for watching events.
package eventwatcher

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/backoffutils"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// EventWatcher is a service for watching for events and forwarding those on C.
// It connects to a server using ApiConnectionDetails and subscribes to events (queue, jobSetName).
type EventWatcher struct {
	Out                  io.Writer
	Queue                string
	JobSetName           string
	ApiConnectionDetails *client.ApiConnectionDetails
	C                    chan *api.EventMessage
	// BackoffExponential produces increasing intervals for each retry attempt.
	//
	// The scalar is multiplied times 2 raised to the current attempt. So the first
	// retry with a scalar of 100ms is 100ms, while the 5th attempt would be 1.6s.
	BackoffExponential time.Duration
	// MaxRetries is the number of consecutive retries until the watcher gives up.
	MaxRetries uint
}

func New(queue string, jobSetName string, apiConnectionDetails *client.ApiConnectionDetails) *EventWatcher {
	return &EventWatcher{
		Out:                  os.Stdout,
		Queue:                queue,
		JobSetName:           jobSetName,
		ApiConnectionDetails: apiConnectionDetails,
		C:                    make(chan *api.EventMessage),
		BackoffExponential:   time.Second,
		MaxRetries:           6,
	}
}

// Run starts the service.
func (srv *EventWatcher) Run(ctx context.Context) error {
	var attempt uint
	var fromMessageId string
	var lastErr error
	for attempt < srv.MaxRetries {
		if err := srv.waitRetryBackoff(ctx, attempt); err != nil {
			return err
		}
		err := client.WithEventClient(srv.ApiConnectionDetails, func(c api.EventClient) error {
			stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
				Id:            srv.JobSetName,
				Queue:         srv.Queue,
				FromMessageId: fromMessageId,
				Watch:         true,
			})
			if err != nil {
				return err
			}
			for {
				msg, err := stream.Recv()
				if err != nil {
					return err
				}
				attempt = 0
				fromMessageId = msg.GetId()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case srv.C <- msg.Message:
				}
			}
		})
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if status.Code(err) == codes.Canceled {
			return err
		}

		attempt++
		lastErr = err
		fmt.Fprintf(srv.Out, "EventWatcher stream broken: %s\n", err)
	}
	return lastErr
}

func (srv *EventWatcher) waitRetryBackoff(ctx context.Context, attempt uint) error {
	var waitTime time.Duration
	if attempt > 0 {
		waitTime = srv.BackoffExponential * time.Duration(backoffutils.ExponentBase2(attempt))
	}
	if waitTime > 0 {
		fmt.Fprintf(srv.Out, "EventWatcher attempt %d, backoff for %v\n", attempt, waitTime)
		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

// ErrUnexpectedEvent indicates the wrong event type was received.
type ErrUnexpectedEvent struct {
	jobId    string
	expected *api.EventMessage
	actual   *api.EventMessage
	message  string
}

func (err *ErrUnexpectedEvent) Error() string {
	baseMsg := fmt.Sprintf(
		"unexpected event for job %s: expected event of type %T, but got %+v",
		err.jobId, err.expected.Events, err.actual.Events,
	)
	if err.message == "" {
		return baseMsg
	}
	return fmt.Sprintf("%s: %s", baseMsg, err.message)
}

// AssertEvents compares the events received for each job with the expected events.
func AssertEvents(ctx context.Context, c chan *api.EventMessage, jobIds map[string]bool, expected []*api.EventMessage) error {

	// terminatedByJobId indicates for which jobs we've received a terminal event.
	// Initialise it by copying the jobIds map.
	terminatedByJobId := maps.Clone(jobIds)

	// Track which events have been seen for each job.
	indexByJobId := make(map[string]int)
	numDone := 0

	// Receive events until all expected events have been received
	// or an unexpected event has been received.
	for {
		select {
		case <-ctx.Done():
			s := assertEventErrorString(expected, indexByJobId)
			if s != "" {
				return errors.Errorf("test exited before receiving all expected events; %s: %s", s, ctx.Err())
			} else {
				return errors.Errorf("test exited before receiving all expected events: %s", ctx.Err())
			}
		case actual := <-c:
			if len(expected) == 0 {
				// Run forever if expected events is empty.
				break
			}

			actualJobId := api.JobIdFromApiEvent(actual)
			_, ok := jobIds[actualJobId]
			if !ok {
				// Unrecognised job id.
				break
			}

			// Record terminated jobs.
			if isTerminalEvent(actual) {
				terminatedByJobId[actualJobId] = true
			}

			i := indexByJobId[actualJobId]
			if i < len(expected) && reflect.TypeOf(actual.Events) == reflect.TypeOf(expected[i].Events) {
				if err := assertEvent(expected[i], actual); err != nil {
					return err
				}
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
					jobId:    actualJobId,
					expected: expected[i],
					actual:   actual,
				}
			}
		}
	}
}

func assertEventErrorString(expected []*api.EventMessage, indexByJobId map[string]int) string {
	countByIndex := make(map[int]int)
	for _, i := range indexByJobId {
		countByIndex[i] = +1
	}
	elems := make([]string, 0, len(countByIndex))
	for i, c := range countByIndex {
		received := expected[:i]
		missing := expected[i:]
		if len(missing) == 0 {
			continue
		}
		elem := fmt.Sprintf(
			"%s received but %s missing for %d job(s)",
			api.ShortStringFromEventMessages(received), api.ShortStringFromEventMessages(missing), c,
		)
		elems = append(elems, elem)
	}
	return strings.Join(elems, ", ")
}

func isTerminalEvent(msg *api.EventMessage) bool {
	switch msg.Events.(type) {
	case *api.EventMessage_Failed:
		return true
	case *api.EventMessage_Succeeded:
		return true
	case *api.EventMessage_Cancelled:
		return true
	case *api.EventMessage_DuplicateFound:
		return true
	}
	return false
}

// ErrorOnNoActiveJobs returns an error if there are no active jobs.
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
			} else if e := msg.GetCancelled(); e != nil {
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
				return errors.New("no jobs active")
			}
		}
	}
}

// ErrorOnFailed returns an error on job failure.
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
			return ctx.Err()
		case <-ctx.Done(): // errgroup cancelled
			return g.Wait()
		case msg := <-C:
			if ingressInfo := msg.GetIngressInfo(); ingressInfo != nil {
				for _, host := range ingressInfo.IngressAddresses {
					ctxWithTimeout, _ := context.WithTimeout(ctx, time.Minute)
					host := host
					g.Go(func() error { return getFromIngress(ctxWithTimeout, host) })
				}
			}
		}
	}
}

func getFromIngress(ctx context.Context, host string) error {
	ingressUrl := os.Getenv("ARMADA_EXECUTOR_INGRESS_URL")
	ingressUseTls := strings.TrimSpace(strings.ToLower(os.Getenv("ARMADA_EXECUTOR_USE_TLS")))
	if ingressUrl == "" {
		if ingressUseTls != "" && ingressUseTls != "false" && ingressUseTls != "0" {
			ingressUrl = "https://" + host
		} else {
			ingressUrl = "http://" + host
		}
	}

	// The ingress info messages can't convey which port ingress are handled on (only the url).
	// (The assumption is that ingress is always handled on port 80.)
	// Here, we override this with an environment variable so we can test against local Kind clusters,
	// which may handle ingress on an arbitrary port.
	ingressPort := os.Getenv("ARMADA_EXECUTOR_INGRESS_PORT")
	if ingressPort == "" {
		ingressPort = "80"
	}

	// Make a get request to test that the ingress works.
	// This assumes that whatever the ingress points to responds.
	// We don't care about certificate validity, just if connecting is possible.
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
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
			time.Sleep(time.Second)
			httpRes, err := httpClient.Do(httpReq)
			if err != nil {
				requestErr = errors.Errorf("GET request failed: %s", err)
				break
			}
			httpRes.Body.Close()

			if httpRes.StatusCode != 200 {
				requestErr = errors.Errorf(
					"GET request failed for %s:%s: %d",
					ingressUrl, ingressPort, httpRes.StatusCode,
				)
				break
			}

			return nil
		}
	}
}
