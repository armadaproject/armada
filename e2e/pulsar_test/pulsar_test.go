package pulsar_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client"
)

// Pulsar configuration. Must be manually reconciled with changes to the test setup or Armada.
const (
	pulsarUrl            = "pulsar://localhost:6650"
	pulsarTopic          = "events"
	pulsarSubscription   = "e2e-test"
	armadaUrl            = "localhost:50051"
	armadaQueueName      = "e2e-test-queue"
	armadaUserId         = "anonymous"
	defaultPulsarTimeout = 60 * time.Second
)

// We setup kind to expose ingresses on this ULR.
const ingressUrl = "http://localhost:5001"

// Armada exposes all ingresses on this path.
// Routing to the correct service is done using the hostname header.
const ingressPath = "/"

// Namespace created by the test setup. Used when submitting test jobs.
const userNamespace = "personal-anonymous"

// The submit server should automatically add default tolerations.
// These must be manually updated to match the default tolerations in the server config.
var expectedTolerations = []v1.Toleration{
	{
		Key:               "example.com/default_toleration",
		Operator:          v1.TolerationOpEqual,
		Value:             "true",
		Effect:            v1.TaintEffectNoSchedule,
		TolerationSeconds: nil,
	},
}

// Test publishing and receiving a message to/from Pulsar.
func TestPublishReceive(t *testing.T) {
	err := withSetup(func(ctx context.Context, _ api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("hello"),
		})
		if err != nil {
			return err
		}

		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		msg, err := consumer.Receive(ctxWithTimeout)
		if err != nil {
			return err
		}
		assert.Equal(t, "hello", string(msg.Payload()))

		return nil
	})
	assert.NoError(t, err)
}

// Test that submitting many jobs results in the correct sequence of Pulsar message being produced for each job.
func TestSubmitJobs(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		numJobs := 2
		req := createJobSubmitRequest(numJobs)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		numEventsExpected := numJobs * 6
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence := flattenSequences(sequences)
		if ok := assert.NotNil(t, sequence); !ok {
			return nil
		}

		for i, resi := range res.JobResponseItems {
			reqi := req.JobRequestItems[i]

			jobId, err := armadaevents.ProtoUuidFromUlidString(resi.JobId)
			if err != nil {
				return err
			}

			expected := armadaevents.ExpectedSequenceFromRequestItem(armadaQueueName, armadaUserId, userNamespace, req.JobSetId, jobId, reqi)
			actual, err := filterSequenceByJobId(sequence, jobId)
			if err != nil {
				return err
			}
			if !isSequencef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)) {
				t.FailNow()
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

// TODO: Make testsuite test. Or unit test.
func TestDedup(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		numJobs := 2
		clientId := uuid.New().String()
		originalJobIds := make([]string, numJobs)

		// The first time, all jobs should be submitted as-is.
		req := createJobSubmitRequestWithClientId(numJobs, clientId)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		for i := 0; i < numJobs; i++ {
			originalJobIds[i] = res.JobResponseItems[i].GetJobId()
		}

		numEventsExpected := numJobs * 6
		_, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		// The second time, job ids should be replaced with the original ids.
		req = createJobSubmitRequestWithClientId(numJobs, clientId)
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		res, err = client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		for i := 0; i < numJobs; i++ {
			assert.Equal(t, originalJobIds[i], res.JobResponseItems[i].GetJobId())
		}

		numEventsExpected = numJobs // one duplicate detected message per job
		_, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		// Here, some ids should be replaced and some should be new.
		req = createJobSubmitRequestWithClientId(numJobs, clientId)
		req2 := createJobSubmitRequestWithClientId(numJobs, uuid.New().String())
		req.JobRequestItems = append(req.JobRequestItems, req2.JobRequestItems...)
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		res, err = client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, 2*numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		for i := 0; i < numJobs; i++ {
			assert.Equal(t, originalJobIds[i], res.JobResponseItems[i].GetJobId())
		}

		numEventsExpected = numJobs*6 + numJobs
		_, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		return nil
	})
	assert.NoError(t, err)
}

// Test submitting several jobs, cancelling all of them, and checking that at least 1 is cancelled.
func TestSubmitCancelJobs(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		// The ingress job runs until canceled.
		req := createJobSubmitRequestWithIngress()
		numJobs := len(req.JobRequestItems)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		// Wait for the jobs to be running (submitted, leased, assigned, running, ingress info).
		numEventsExpected := numJobs * 5
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence := flattenSequences(sequences)
		if ok := assert.NotNil(t, sequence); !ok {
			return nil
		}

		// Cancel the jobs
		for _, r := range res.JobResponseItems {
			ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
			res, err := client.CancelJobs(ctxWithTimeout, &api.JobCancelRequest{
				JobId: r.JobId,
				// Leave JobSetId and Queue empty to check that these are auto-populated.
			})
			if !assert.NoError(t, err) {
				return nil
			}
			assert.Equal(t, []string{r.JobId}, res.CancelledIds)
		}

		// Check that the job is cancelled (cancel, cancelled).
		numEventsExpected = numJobs * 2
		sequences, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence = flattenSequences(sequences)
		if ok := assert.NotNil(t, sequence); !ok {
			return nil
		}

		for _, resi := range res.JobResponseItems {
			jobId, err := armadaevents.ProtoUuidFromUlidString(resi.JobId)
			if err != nil {
				return err
			}

			actual, err := filterSequenceByJobId(sequence, jobId)
			if err != nil {
				return err
			}

			expected := &armadaevents.EventSequence{
				Queue:      armadaQueueName,
				JobSetName: req.JobSetId,
				UserId:     armadaUserId,
				Events: []*armadaevents.EventSequence_Event{
					{Event: &armadaevents.EventSequence_Event_CancelJob{}},
					{Event: &armadaevents.EventSequence_Event_CancelledJob{}},
				},
			}
			if ok := isSequenceTypef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)); !ok {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

// Test cancelling a job set.
func TestSubmitCancelJobSet(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		// Submit a few jobs that fail after a few seconds
		numJobs := 2
		req := createJobSubmitRequestWithError(numJobs)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		_, err = client.CancelJobs(ctxWithTimeout, &api.JobCancelRequest{
			JobSetId: req.JobSetId,
			Queue:    req.Queue,
		})
		if !assert.NoError(t, err) {
			return nil
		}

		// Test that we get submit, cancel job set, and cancelled messages.
		numEventsExpected := numJobs + numJobs + numJobs
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		actual := flattenSequences(sequences)
		if ok := assert.NotNil(t, actual); !ok {
			return nil
		}

		expected := &armadaevents.EventSequence{
			Queue:      armadaQueueName,
			JobSetName: req.JobSetId,
			UserId:     armadaUserId,
			Events:     []*armadaevents.EventSequence_Event{},
		}
		for range res.JobResponseItems {
			expected.Events = append(
				expected.Events,
				&armadaevents.EventSequence_Event{
					Event: &armadaevents.EventSequence_Event_SubmitJob{},
				},
			)
		}
		for range res.JobResponseItems {
			expected.Events = append(
				expected.Events,
				&armadaevents.EventSequence_Event{
					Event: &armadaevents.EventSequence_Event_CancelJob{},
				},
			)
		}
		for range res.JobResponseItems {
			expected.Events = append(
				expected.Events,
				&armadaevents.EventSequence_Event{
					Event: &armadaevents.EventSequence_Event_CancelledJob{},
				},
			)
		}
		if ok := isSequenceTypef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)); !ok {
			return nil
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestIngress(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		req := createJobSubmitRequestWithIngress()
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, 1, len(res.JobResponseItems)); !ok {
			return nil
		}

		numEventsExpected := 5
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence := flattenSequences(sequences)
		if !assert.NotNil(t, sequence) {
			return nil
		}

		if !assert.Equal(t, numEventsExpected, len(sequence.Events)) {
			t.FailNow()
		}

		// Armada generated a special event with info on how to connect to the ingress.
		ingressInfoEvent, ok := sequence.Events[numEventsExpected-1].GetEvent().(*armadaevents.EventSequence_Event_StandaloneIngressInfo)
		if !assert.True(t, ok) {
			t.FailNow()
		}
		ingressInfo := ingressInfoEvent.StandaloneIngressInfo

		actualJobId, err := armadaevents.UlidStringFromProtoUuid(ingressInfo.GetJobId())
		if err != nil {
			return err
		}
		assert.Equal(t, res.JobResponseItems[0].JobId, actualJobId)

		// Hostname used to route requests to the service setup for the created pod.
		containerPort := int32(80)
		host, ok := ingressInfo.IngressAddresses[containerPort]
		if !assert.True(t, ok) {
			t.FailNow()
		}

		// It takes a few seconds for the ingress to become active.
		// Ideally, we would make repeated requests up to some max timeout instead of using a constant 10s.
		time.Sleep(10 * time.Second)

		// Make a get request to this hostname to verify that we get a response from the pod.
		httpClient := &http.Client{}
		httpReq, err := http.NewRequest("GET", ingressUrl+ingressPath, nil)
		if err != nil {
			return err
		}
		httpReq.Host = host
		httpRes, err := httpClient.Do(httpReq)
		if err != nil {
			return err
		}
		httpResBytes, err := ioutil.ReadAll(httpRes.Body)
		if err != nil {
			return err
		}
		assert.Contains(t, string(httpResBytes), "If you see this page, the nginx web server is successfully installed")

		// Cancel the job to clean it up.
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		_, err = client.CancelJobs(ctxWithTimeout, &api.JobCancelRequest{
			JobId:    res.JobResponseItems[0].JobId,
			JobSetId: req.JobSetId,
			Queue:    req.Queue,
		})
		if !assert.NoError(t, err) {
			return nil
		}

		numEventsExpected = 3
		sequences, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence = flattenSequences(sequences)
		if !assert.NotNil(t, sequence) {
			return nil
		}

		if !assert.Equal(t, numEventsExpected, len(sequence.Events)) {
			t.FailNow()
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestService(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		// Create a job running an nginx server accessible via a headless service.
		req := createJobSubmitRequestWithService()
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, 1, len(res.JobResponseItems)); !ok {
			return nil
		}

		numEventsExpected := 5
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence := flattenSequences(sequences)
		if !assert.NotNil(t, sequence) {
			return nil
		}

		if !assert.Equal(t, numEventsExpected, len(sequence.Events)) {
			t.FailNow()
		}

		// It takes a few seconds for the service to become active.
		// Ideally, we would make repeated requests up to some max timeout instead of using a constant 10s.
		time.Sleep(10 * time.Second)

		// Get the ip of the nginx pod via the k8s api.
		podIndex := 0
		endpointName := fmt.Sprintf("armada-%s-%d-headless", res.GetJobResponseItems()[0].JobId, podIndex)
		out, err := exec.Command("kubectl", "get", "endpoints", endpointName, "--namespace", userNamespace, "-o", "jsonpath='{.subsets[0].addresses[0].ip}'").
			Output()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		address := strings.ReplaceAll(string(out), "'", "") + ":80"

		// Submit a new job that queries that ip using wget.
		wgetReq := createWgetJobRequest(address)
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		wgetRes, err := client.SubmitJobs(ctxWithTimeout, wgetReq)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, 1, len(wgetRes.JobResponseItems)); !ok {
			return nil
		}

		// Check that the wget job completes successfully.
		numEventsExpected = 5
		sequences, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, wgetReq.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence = flattenSequences(sequences)
		if !assert.NotNil(t, sequence) {
			return nil
		}

		if !assert.Equal(t, numEventsExpected, len(sequence.Events)) {
			t.FailNow()
		}

		_, ok := sequence.Events[numEventsExpected-1].GetEvent().(*armadaevents.EventSequence_Event_JobSucceeded)
		assert.True(t, ok)

		// Cancel the original job (i.e., the nginx job).
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		_, err = client.CancelJobs(ctxWithTimeout, &api.JobCancelRequest{
			JobId:    res.JobResponseItems[0].JobId,
			JobSetId: req.JobSetId,
			Queue:    req.Queue,
		})
		if !assert.NoError(t, err) {
			return nil
		}

		numEventsExpected = 3
		sequences, err = receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence = flattenSequences(sequences)
		if !assert.NotNil(t, sequence) {
			return nil
		}

		if !assert.Equal(t, numEventsExpected, len(sequence.Events)) {
			t.FailNow()
		}

		return nil
	})
	assert.NoError(t, err)
}

// Test that submitting many jobs results in the correct sequence of Pulsar message being produced for each job.
// For jobs that contain multiple PodSpecs, services, and ingresses.
func TestSubmitJobsWithEverything(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		numJobs := 1
		req := createJobSubmitRequestWithEverything(numJobs)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		numEventsExpected := numJobs * 7
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence := flattenSequences(sequences)
		if ok := assert.NotNil(t, sequence); !ok {
			return nil
		}

		for i, resi := range res.JobResponseItems {
			reqi := req.JobRequestItems[i]

			jobId, err := armadaevents.ProtoUuidFromUlidString(resi.JobId)
			if err != nil {
				return err
			}

			expected := armadaevents.ExpectedSequenceFromRequestItem(armadaQueueName, armadaUserId, userNamespace, req.JobSetId, jobId, reqi)
			actual, err := filterSequenceByJobId(sequence, jobId)
			if err != nil {
				return err
			}

			// Because the order of the ingress info messages varies (e.g., they may arrive after the job has completed),
			// we filter those out and check them separately.
			actual, standaloneIngressInfos, err := filterOutStandaloneIngressInfo(actual)
			if err != nil {
				return err
			}

			if ok := isSequencef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)); !ok {
				return nil
			}

			if !assert.Equal(t, 1, len(standaloneIngressInfos)) {
				return nil
			}
			standaloneIngressInfo := standaloneIngressInfos[0]
			fmt.Printf("standaloneIngressInfo:\n%+v\n", standaloneIngressInfo)
			assert.Equal(t, standaloneIngressInfo.ObjectMeta.ExecutorId, "Cluster1")
			assert.Equal(t, standaloneIngressInfo.ObjectMeta.Namespace, "personal-anonymous")
			assert.Equal(t, standaloneIngressInfo.PodNamespace, "personal-anonymous")
			_, ok := standaloneIngressInfo.IngressAddresses[5000]
			assert.True(t, ok)
			_, ok = standaloneIngressInfo.IngressAddresses[6000]
			assert.True(t, ok)
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestSubmitJobWithError(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		// Submit a few jobs that fail after a few seconds
		numJobs := 1
		req := createJobSubmitRequestWithError(numJobs)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		// Test that we get errors messages.
		numEventsExpected := numJobs * 5
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, defaultPulsarTimeout)
		if err != nil {
			return err
		}

		sequence := flattenSequences(sequences)
		if ok := assert.NotNil(t, sequence); !ok {
			return nil
		}

		for _, resi := range res.JobResponseItems {

			jobId, err := armadaevents.ProtoUuidFromUlidString(resi.JobId)
			if err != nil {
				return err
			}

			actual, err := filterSequenceByJobId(sequence, jobId)
			if err != nil {
				return err
			}
			if !assert.NotEmpty(t, actual.Events) {
				return nil
			}
			expected := &armadaevents.EventSequence{
				Queue:      req.Queue,
				JobSetName: req.JobSetId,
				UserId:     armadaUserId,
				Events: []*armadaevents.EventSequence_Event{
					{
						Event: &armadaevents.EventSequence_Event_JobRunErrors{
							JobRunErrors: &armadaevents.JobRunErrors{
								JobId: jobId,
							},
						},
					},
					{
						Event: &armadaevents.EventSequence_Event_JobErrors{
							JobErrors: &armadaevents.JobErrors{
								JobId: jobId,
							},
						},
					},
				},
			}

			// Only check the two final events.
			actual.Events = actual.Events[len(actual.Events)-2:]
			if !isSequencef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)) {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

func isSequenceTypef(t *testing.T, expected *armadaevents.EventSequence, actual *armadaevents.EventSequence, msg string, args ...interface{}) (ok bool) {
	defer func() {
		if !ok && msg != "" {
			t.Logf(msg, args...)
		}
	}()
	if ok = assert.Equal(t, len(expected.Events), len(actual.Events)); !ok {
		return false
	}
	for i, expectedEvent := range expected.Events {
		actualEvent := actual.Events[i]
		if ok = assert.IsType(t, expectedEvent.Event, actualEvent.Event); !ok {
			return false
		}
	}
	return true
}

// Like isSequence, but logs msg if a comparison fails.
func isSequencef(t *testing.T, expected *armadaevents.EventSequence, actual *armadaevents.EventSequence, msg string, args ...interface{}) bool {
	ok := true
	defer func() {
		if !ok && msg != "" {
			t.Logf(msg, args...)
		}
	}()
	ok = ok && assert.NotNil(t, expected)
	ok = ok && assert.NotNil(t, actual)
	ok = ok && assert.Equal(t, expected.Queue, actual.Queue)
	ok = ok && assert.Equal(t, expected.JobSetName, actual.JobSetName)
	ok = ok && assert.Equal(t, expected.UserId, actual.UserId)
	ok = ok && assert.Equal(t, len(expected.Events), len(actual.Events))
	if len(expected.Events) == len(actual.Events) {
		for i, expectedEvent := range expected.Events {
			actualEvent := actual.Events[i]
			ok = ok && isEventf(t, expectedEvent, actualEvent, "%d-th event differed: %s", i, actualEvent)
		}
	}
	return ok
}

// Compare an actual event with an expected event.
// Only compares the subset of fields relevant for testing.
func isEventf(t *testing.T, expected *armadaevents.EventSequence_Event, actual *armadaevents.EventSequence_Event, msg string, args ...interface{}) bool {
	ok := true
	defer func() {
		if !ok && msg != "" {
			t.Logf(msg, args...)
		}
	}()
	if ok = ok && assert.IsType(t, expected.Event, actual.Event); !ok {
		return ok
	}

	// If the expected event includes a jobId, the actual event must include the same jobId.
	expectedJobId, err := armadaevents.JobIdFromEvent(expected)
	if err == nil {
		actualJobId, err := armadaevents.JobIdFromEvent(actual)
		if err == nil { // Ignore for events without a jobId (e.g., cancelJobSet).
			if ok = ok && assert.Equal(t, expectedJobId, actualJobId); ok {
				return ok
			}
		}
	}

	switch expectedEvent := expected.Event.(type) {
	case *armadaevents.EventSequence_Event_SubmitJob:
		actualEvent, ok := actual.Event.(*armadaevents.EventSequence_Event_SubmitJob)
		if ok = ok && assert.True(t, ok); !ok {
			return ok
		}
		ok = ok && assert.Equal(t, *expectedEvent.SubmitJob.JobId, *actualEvent.SubmitJob.JobId)
		ok = ok && assert.Equal(t, expectedEvent.SubmitJob.DeduplicationId, actualEvent.SubmitJob.DeduplicationId)
		ok = ok && assert.Equal(t, expectedEvent.SubmitJob.Priority, actualEvent.SubmitJob.Priority)
		ok = ok && assert.IsType(t, expectedEvent.SubmitJob.MainObject.Object, actualEvent.SubmitJob.MainObject.Object)
		ok = ok && assert.NotNil(t, expectedEvent.SubmitJob.ObjectMeta)
		ok = ok && assert.Equal(t, expectedEvent.SubmitJob.ObjectMeta.Namespace, actualEvent.SubmitJob.ObjectMeta.Namespace)

		expectedObjectCounts := countObjectTypes(expectedEvent.SubmitJob.Objects)
		actualObjectCounts := countObjectTypes(actualEvent.SubmitJob.Objects)
		ok = ok && assert.Equal(t, expectedObjectCounts, actualObjectCounts)

		ok = ok && assert.NotEmpty(t, actualEvent.SubmitJob.MainObject)

		// The main object must be a podspec.
		mainPodSpec, isPodSpec := (actualEvent.SubmitJob.MainObject.Object).(*armadaevents.KubernetesMainObject_PodSpec)
		ok = ok && assert.True(t, isPodSpec)

		// Collect all podspecs in the job.
		podSpecs := make([]*v1.PodSpec, 0)
		podSpecs = append(podSpecs, mainPodSpec.PodSpec.PodSpec)
		for _, object := range actualEvent.SubmitJob.Objects {
			if podSpec, isPodSpec := (object.Object).(*armadaevents.KubernetesObject_PodSpec); isPodSpec {
				podSpecs = append(podSpecs, podSpec.PodSpec.PodSpec)
			}
		}

		// Test that all podspecs have the expected default tolerations.
		for _, podSpec := range podSpecs {
			ok = ok && assert.Equal(t, expectedTolerations, podSpec.Tolerations)
		}

		return ok
	case *armadaevents.EventSequence_Event_ReprioritiseJob:
	case *armadaevents.EventSequence_Event_CancelJob:
	case *armadaevents.EventSequence_Event_JobSucceeded:
	case *armadaevents.EventSequence_Event_JobRunSucceeded:
	case *armadaevents.EventSequence_Event_JobRunLeased:
	case *armadaevents.EventSequence_Event_JobRunAssigned:
	case *armadaevents.EventSequence_Event_JobRunRunning:
	case *armadaevents.EventSequence_Event_JobRunErrors:
	}
	return true
}

// countObjectTypes returns a map from object type (as a string) to the number of objects of that type.
func countObjectTypes(objects []*armadaevents.KubernetesObject) map[string]int {
	result := make(map[string]int)
	for _, object := range objects {
		typeName := fmt.Sprintf("%T", object.Object)
		count, _ := result[typeName]
		result[typeName] = count + 1
	}
	return result
}

// receiveJobSetSequence receives messages from Pulsar, discarding any messages not for queue and jobSetName.
// The events contained in the remaining messages are collected in a single sequence, which is returned.
func receiveJobSetSequences(
	ctx context.Context,
	consumer pulsar.Consumer,
	queue string,
	jobSetName string,
	maxEvents int,
	timeout time.Duration,
) (sequences []*armadaevents.EventSequence, err error) {
	sequences = make([]*armadaevents.EventSequence, 0)
	numEvents := 0
	for numEvents < maxEvents {
		ctxWithTimeout, _ := context.WithTimeout(ctx, timeout)
		var msg pulsar.Message
		msg, err = consumer.Receive(ctxWithTimeout)
		if err == context.DeadlineExceeded {
			fmt.Println("Timed out waiting for event")
			err = nil // Timeout is expected; ignore.
			return
		} else if err != nil {
			fmt.Println("Pulsar receive error", err)
			continue
		}
		consumer.Ack(msg)

		sequence := &armadaevents.EventSequence{}
		err = proto.Unmarshal(msg.Payload(), sequence)
		if err != nil {
			fmt.Println("Sequence unmarshalling error", err)
			continue
		}
		fmt.Printf("Received sequence %s\n", sequence)

		if sequence.Queue != queue || sequence.JobSetName != jobSetName {
			fmt.Println("Skipping sequence")
			continue
		}

		numEvents += len(sequence.Events)
		sequences = append(sequences, sequence)
	}
	return
}

// concatenateSequences returns a new sequence containing all events in the provided slice of sequences.
func flattenSequences(sequences []*armadaevents.EventSequence) *armadaevents.EventSequence {
	if len(sequences) == 0 {
		return nil
	}
	result := &armadaevents.EventSequence{
		Queue:      sequences[0].Queue,
		JobSetName: sequences[0].JobSetName,
		UserId:     sequences[0].UserId,
		Events:     make([]*armadaevents.EventSequence_Event, 0),
	}
	for _, sequence := range sequences {
		result.Events = append(result.Events, sequence.Events...)
	}
	return result
}

// filterSequenceByJobId returns a new event sequence composed of the events for the job with the specified id.
func filterSequenceByJobId(sequence *armadaevents.EventSequence, id *armadaevents.Uuid) (*armadaevents.EventSequence, error) {
	result := &armadaevents.EventSequence{
		Queue:      sequence.Queue,
		JobSetName: sequence.JobSetName,
		UserId:     sequence.UserId,
		Events:     make([]*armadaevents.EventSequence_Event, 0),
	}
	for _, e := range sequence.Events {
		jobId, err := armadaevents.JobIdFromEvent(e)
		if errors.Is(err, &armadaerrors.ErrInvalidArgument{}) {
			continue
		} else if err != nil {
			return nil, err
		}
		if *jobId != *id {
			continue
		}
		result.Events = append(result.Events, e)
	}
	return result, nil
}

// filterOutStandaloneIngressInfo removed StandaloneIngressInfo events from the sequence.
// We may wish to remove them because these events may arrive in different order (e.g., it could arrive after JobRunSucceeded).
func filterOutStandaloneIngressInfo(sequence *armadaevents.EventSequence) (*armadaevents.EventSequence, []*armadaevents.StandaloneIngressInfo, error) {
	result := &armadaevents.EventSequence{
		Queue:      sequence.Queue,
		JobSetName: sequence.JobSetName,
		UserId:     sequence.UserId,
		Events:     make([]*armadaevents.EventSequence_Event, 0),
	}
	standaloneIngressInfos := make([]*armadaevents.StandaloneIngressInfo, 0)
	for _, e := range sequence.Events {
		if ingressInfo, ok := e.Event.(*armadaevents.EventSequence_Event_StandaloneIngressInfo); ok {
			standaloneIngressInfos = append(standaloneIngressInfos, ingressInfo.StandaloneIngressInfo)
			continue
		}
		result.Events = append(result.Events, e)
	}
	return result, standaloneIngressInfos, nil
}

// Create a job submit request for testing.
func createJobSubmitRequest(numJobs int) *api.JobSubmitRequest {
	return createJobSubmitRequestWithClientId(numJobs, uuid.New().String())
}

// Create a job submit request for testing.
func createJobSubmitRequestWithClientId(numJobs int, clientId string) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	items := make([]*api.JobSubmitRequestItem, numJobs, numJobs)
	for i := 0; i < numJobs; i++ {
		itemClientId := clientId
		if itemClientId != "" {
			itemClientId = fmt.Sprintf("%s-%d", itemClientId, i)
		}
		items[i] = &api.JobSubmitRequestItem{
			Namespace: userNamespace,
			Priority:  1,
			ClientId:  itemClientId,
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "container1",
						Image: "alpine:3.10",
						Args:  []string{"sleep", "5s"},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
							Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
						},
					},
				},
			},
		}
	}
	return &api.JobSubmitRequest{
		Queue:           armadaQueueName,
		JobSetId:        util.NewULID(),
		JobRequestItems: items,
	}
}

// Return a job request with a container that queries the specified address using wget.
func createWgetJobRequest(address string) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	items := []*api.JobSubmitRequestItem{
		{
			Namespace: userNamespace,
			Priority:  1,
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "wget",
						Image: "alpine:3.10",
						Args:  []string{"wget", address, "--timeout=5"}, // Queried from the k8s services API
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
							Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
						},
					},
				},
			},
		},
	}
	return &api.JobSubmitRequest{
		Queue:           armadaQueueName,
		JobSetId:        util.NewULID(),
		JobRequestItems: items,
	}
}

// Create a job submit request with an ingress.
func createJobSubmitRequestWithIngress() *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	items := make([]*api.JobSubmitRequestItem, 1)

	items[0] = &api.JobSubmitRequestItem{
		Namespace: userNamespace,
		Priority:  1,
		ClientId:  uuid.New().String(), // So we can test that we get back the right thing
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "nginx",
					Image: "nginx:1.21.6",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
						Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
					},
					// Armada silently deletes services/ingresses unless the main pod exposes those.
					// Hence, we need to expose the following ports.
					Ports: []v1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      v1.ProtocolTCP,
							Name:          "port",
						},
					},
				},
			},
		},
		Ingress: []*api.IngressConfig{
			{
				Ports: []uint32{80},
			},
		},
	}
	return &api.JobSubmitRequest{
		Queue:           armadaQueueName,
		JobSetId:        util.NewULID(),
		JobRequestItems: items,
	}
}

// Create a job submit request with services.
func createJobSubmitRequestWithService() *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	items := make([]*api.JobSubmitRequestItem, 1)

	items[0] = &api.JobSubmitRequestItem{
		Namespace: userNamespace,
		Priority:  1,
		ClientId:  uuid.New().String(), // So we can test that we get back the right thing
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "nginx",
					Image: "nginx:1.21.6",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
						Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
					},
					// Armada silently deletes services/ingresses unless the main pod exposes those.
					// Hence, we need to expose the following ports.
					Ports: []v1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      v1.ProtocolTCP,
							Name:          "port80",
						},
						{
							ContainerPort: 6000,
							Protocol:      v1.ProtocolTCP,
							Name:          "port6000",
						},
					},
				},
			},
		},
		Services: []*api.ServiceConfig{
			{
				Type:  api.ServiceType_Headless,
				Ports: []uint32{80},
			},
			{
				Type:  api.ServiceType_NodePort,
				Ports: []uint32{80},
			},
		},
	}

	return &api.JobSubmitRequest{
		Queue:           armadaQueueName,
		JobSetId:        util.NewULID(),
		JobRequestItems: items,
	}
}

// Create a job submit request with ingresses and services for testing.
func createJobSubmitRequestWithEverything(numJobs int) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	items := make([]*api.JobSubmitRequestItem, numJobs, numJobs)
	for i := 0; i < numJobs; i++ {
		items[i] = &api.JobSubmitRequestItem{
			Namespace: userNamespace,
			Priority:  1,
			ClientId:  uuid.New().String(), // So we can test that we get back the right thing
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "container1",
						Image: "alpine:3.10",
						Args:  []string{"sleep", "5s"},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
							Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
						},
						// Armada silently deletes services/ingresses unless the main pod exposes those.
						// Hence, we need to expose the following ports.
						Ports: []v1.ContainerPort{
							{
								ContainerPort: 5000,
								Protocol:      v1.ProtocolTCP,
								Name:          "port5000",
							},
							{
								ContainerPort: 6000,
								Protocol:      v1.ProtocolTCP,
								Name:          "port6000",
							},
							{
								ContainerPort: 7000,
								Protocol:      v1.ProtocolTCP,
								Name:          "port7000",
							},
						},
					},
				},
			},
			Ingress: []*api.IngressConfig{
				{
					Type:  api.IngressType_Ingress,
					Ports: []uint32{5000},
				},
			},
			Services: []*api.ServiceConfig{
				{
					Type:  api.ServiceType_NodePort,
					Ports: []uint32{6000},
				},
				{
					Type:  api.ServiceType_Headless,
					Ports: []uint32{7000},
				},
			},
		}
	}
	return &api.JobSubmitRequest{
		Queue:           armadaQueueName,
		JobSetId:        util.NewULID(),
		JobRequestItems: items,
	}
}

// Create a job submit request with a job that returns an error after 1 second.
func createJobSubmitRequestWithError(numJobs int) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	items := make([]*api.JobSubmitRequestItem, numJobs, numJobs)
	for i := 0; i < numJobs; i++ {
		items[i] = &api.JobSubmitRequestItem{
			Namespace: userNamespace,
			Priority:  1,
			ClientId:  uuid.New().String(), // So we can test that we get back the right thing
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "container1",
						Image: "alpine:3.10",
						Args:  []string{"sleep", "5s", "&&", "exit", "1"},
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
							Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
						},
					},
				},
			},
		}
	}
	return &api.JobSubmitRequest{
		Queue:           armadaQueueName,
		JobSetId:        util.NewULID(),
		JobRequestItems: items,
	}
}

// Run action with an Armada submit client and a Pulsar producer and consumer.
func withSetup(action func(ctx context.Context, submitClient api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error) error {
	// Connection to the Armada API. To submit API requests.
	conn, err := client.CreateApiConnection(&client.ApiConnectionDetails{ArmadaUrl: armadaUrl})
	if err != nil {
		return errors.WithStack(err)
	}
	defer conn.Close()
	submitClient := api.NewSubmitClient(conn)

	// Create queue needed for tests.
	err = client.CreateQueue(submitClient, &api.Queue{Name: armadaQueueName, PriorityFactor: 1})
	if st, ok := status.FromError(err); ok && st.Code() == codes.AlreadyExists {
		// Queue already exists; we don't need to create it.
	} else if err != nil {
		return errors.WithStack(err)
	}

	// Redirect Pulsar logs to a file since it's very verbose.
	_ = os.Mkdir("../../.test", os.ModePerm)
	f, err := os.OpenFile("../../.test/pulsar.log", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	if err != nil {
		return errors.WithStack(err)
	}
	logger := logrus.StandardLogger() // .WithField("service", "Pulsar")
	logger.Out = f

	// Connection to Pulsar. To check that the correct sequence of messages are produced.
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:              pulsarUrl,
		OperationTimeout: 5 * time.Second,
		Logger:           pulsarlog.NewLoggerWithLogrus(logger),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer pulsarClient.Close()

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: pulsarTopic,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer producer.Close()

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            pulsarTopic,
		SubscriptionName: pulsarSubscription,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	defer consumer.Close()

	// Skip any messages already published to Pulsar.
	for {
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		_, err := consumer.Receive(ctxWithTimeout)
		if err == context.DeadlineExceeded {
			break
		} else if err != nil {
			return errors.WithStack(err)
		}
	}

	return action(context.Background(), submitClient, producer, consumer)
}
