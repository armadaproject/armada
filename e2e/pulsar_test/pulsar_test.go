package pulsar_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/G-Research/armada/pkg/client"
)

// Pulsar configuration. Must be manually reconciled with changes to the test setup or Armada.
const pulsarUrl = "pulsar://localhost:6650"
const pulsarTopic = "jobset-events"
const pulsarSubscription = "e2e-test"
const armadaUrl = "localhost:50051"
const armadaQueueName = "e2e-test-queue"
const armadaUserId = "anonymous"

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
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, 10*time.Second)
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

			expected := expectedSequenceFromRequestItem(req.JobSetId, jobId, reqi)
			actual, err := filterSequenceByJobId(sequence, jobId)
			if err != nil {
				return err
			}
			if ok := isSequencef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)); !ok {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

func TestDedup(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		numJobs := 2
		clientId := uuid.New().String()

		// The first time, all jobs should be submitted.
		req := createJobSubmitRequestWithClientId(numJobs, clientId)
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		res, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
		}

		// The second time, all jobs should be rejected.
		req = createJobSubmitRequestWithClientId(numJobs, clientId)
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		res, err = client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, 0, len(res.JobResponseItems)); !ok {
			return nil
		}

		// Check for partial rejection
		req = createJobSubmitRequestWithClientId(numJobs, clientId)
		req2 := createJobSubmitRequestWithClientId(numJobs, uuid.New().String())
		req.JobRequestItems = append(req.JobRequestItems, req2.JobRequestItems...)
		ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
		res, err = client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		if ok := assert.Equal(t, numJobs, len(res.JobResponseItems)); !ok {
			return nil
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
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, 10*time.Second)
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

			expected := expectedSequenceFromRequestItem(req.JobSetId, jobId, reqi)
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

		// Test that we get errors messages.
		numEventsExpected := numJobs * 4
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, 10*time.Second)
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
			if ok := assert.NotEmpty(t, actual.Events); !ok {
				return nil
			}
			expected := &armadaevents.EventSequence_Event{
				Event: &armadaevents.EventSequence_Event_JobErrors{
					JobErrors: &armadaevents.JobErrors{
						JobId: jobId,
					},
				},
			}
			if ok := isEventf(t, expected, actual.Events[len(actual.Events)-1], "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)); !ok {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

// Test submitting several jobs, cancelling all of them, and checking that at least 1 is cancelled.
func TestSubmitCancelJobs(t *testing.T) {
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

		for _, r := range res.JobResponseItems {
			ctxWithTimeout, _ = context.WithTimeout(context.Background(), time.Second)
			client.CancelJobs(ctxWithTimeout, &api.JobCancelRequest{
				JobId:    r.JobId,
				JobSetId: req.JobSetId,
				Queue:    req.Queue,
			})
		}

		// Test that we get submit, cancel, and cancelled messages.
		numEventsExpected := numJobs * 3
		sequences, err := receiveJobSetSequences(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, 10*time.Second)
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

			expected := &armadaevents.EventSequence{
				Queue:      armadaQueueName,
				JobSetName: req.JobSetId,
				UserId:     armadaUserId,
				Events: []*armadaevents.EventSequence_Event{
					{Event: &armadaevents.EventSequence_Event_SubmitJob{}},
					{Event: &armadaevents.EventSequence_Event_CancelJob{}},
					{Event: &armadaevents.EventSequence_Event_CancelledJob{}},
				}}
			if ok := isSequenceTypef(t, expected, actual, "Event sequence error; printing diff:\n%s", cmp.Diff(expected, actual)); !ok {
				return nil
			}
		}

		return nil
	})
	assert.NoError(t, err)
}

// expectedSequenceFromJobRequestItem returns the expected event sequence for a particular job request and response.
func expectedSequenceFromRequestItem(jobSetName string, jobId *armadaevents.Uuid, reqi *api.JobSubmitRequestItem) *armadaevents.EventSequence {

	// Any objects created for the job in addition to the main object.
	// We only check that the correct number of objects of each type is created.
	// Later, we may wish to also check the fields of the objects.
	objects := make([]*armadaevents.KubernetesObject, 0)

	// Set of ports associated with a service in the submitted job.
	// Because Armada automatically creates services for ingresses with no corresponding service,
	// we need this to create the correct number of services.
	servicePorts := make(map[uint32]bool)

	// One object per service + compute servicePorts
	if reqi.Services != nil {
		for _, service := range reqi.Services {
			objects = append(objects, &armadaevents.KubernetesObject{Object: &armadaevents.KubernetesObject_Service{}})
			for _, port := range service.Ports {
				servicePorts[port] = true
			}
		}
	}

	// Services and ingresses created for the job.
	if reqi.Ingress != nil {
		for _, ingress := range reqi.Ingress {
			objects = append(objects, &armadaevents.KubernetesObject{Object: &armadaevents.KubernetesObject_Ingress{}})

			// Armada automatically creates services as needed by ingresses
			// (each ingress needs to point to a service).
			for _, port := range ingress.Ports {
				if _, ok := servicePorts[port]; !ok {
					objects = append(objects, &armadaevents.KubernetesObject{Object: &armadaevents.KubernetesObject_Service{}})
				}
			}
		}
	}

	// Count the total number of PodSpecs in the job and add one less than that to the additional objects
	// (since one PodSpec is placed into the main object).
	numPodSpecs := 0
	if reqi.PodSpec != nil {
		numPodSpecs++
	}
	if reqi.PodSpecs != nil {
		numPodSpecs += len(reqi.PodSpecs)
	}
	for i := 0; i < numPodSpecs-1; i++ {
		objects = append(objects, &armadaevents.KubernetesObject{Object: &armadaevents.KubernetesObject_PodSpec{
			// The submit server should add some defaults to the submitted podspec.
			PodSpec: &armadaevents.PodSpecWithAvoidList{},
		}})
	}

	return &armadaevents.EventSequence{
		Queue:      armadaQueueName,
		JobSetName: jobSetName,
		UserId:     armadaUserId,
		Events: []*armadaevents.EventSequence_Event{
			{Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: &armadaevents.SubmitJob{
					JobId:           jobId,
					DeduplicationId: reqi.ClientId,
					Priority:        uint32(reqi.Priority),
					ObjectMeta: &armadaevents.ObjectMeta{
						Namespace:    userNamespace,
						Name:         "",
						KubernetesId: "",
						Annotations:  nil,
						Labels:       nil,
					},
					MainObject:      &armadaevents.KubernetesMainObject{Object: &armadaevents.KubernetesMainObject_PodSpec{}},
					Objects:         objects,
					Lifetime:        0,
					AtMostOnce:      false,
					Preemptible:     false,
					ConcurrencySafe: false,
				},
			}},
			{Event: &armadaevents.EventSequence_Event_JobRunLeased{
				JobRunLeased: &armadaevents.JobRunLeased{
					RunId:      nil,
					JobId:      jobId,
					ExecutorId: "",
				},
			}},
			{Event: &armadaevents.EventSequence_Event_JobRunAssigned{
				JobRunAssigned: &armadaevents.JobRunAssigned{
					RunId: nil,
					JobId: jobId,
				},
			}},
			{Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId:         nil,
					JobId:         jobId,
					ResourceInfos: nil,
				},
			}},
			{Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: nil,
					JobId: jobId,
				},
			}},
			{Event: &armadaevents.EventSequence_Event_JobSucceeded{
				JobSucceeded: &armadaevents.JobSucceeded{
					JobId: jobId,
				},
			}},
		},
	}
}

// Compare the event types (but not the contents) of a sequence of events.
func isSequenceType(t *testing.T, expected *armadaevents.EventSequence, actual *armadaevents.EventSequence, msg string, args ...interface{}) (ok bool) {
	return isSequenceTypef(t, expected, actual, "")
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

// Compare an expected sequence of events with the actual sequence.
// Calls into the assert function to make comparison.
// Returns true if the two sequences are equal and false otherwise.
func isSequence(t *testing.T, expected *armadaevents.EventSequence, actual *armadaevents.EventSequence) (ok bool) {
	return isSequencef(t, expected, actual, "")
}

// Like isSequence, but logs msg if a comparison fails.
func isSequencef(t *testing.T, expected *armadaevents.EventSequence, actual *armadaevents.EventSequence, msg string, args ...interface{}) (ok bool) {
	defer func() {
		if !ok && msg != "" {
			t.Logf(msg, args...)
		}
	}()
	if ok = assert.NotNil(t, expected); !ok {
		return false
	}
	if ok = assert.NotNil(t, actual); !ok {
		return false
	}
	if ok = assert.Equal(t, expected.Queue, actual.Queue); !ok {
		return false
	}
	if ok = assert.Equal(t, expected.JobSetName, actual.JobSetName); !ok {
		return false
	}
	if ok = assert.Equal(t, expected.UserId, actual.UserId); !ok {
		return false
	}
	if ok = assert.Equal(t, len(expected.Events), len(actual.Events)); !ok {
		return false
	}
	for i, expectedEvent := range expected.Events {
		actualEvent := actual.Events[i]
		if ok := isEventf(t, expectedEvent, actualEvent, "%d-th event differed: %s", i, actualEvent); !ok {
			return false
		}
	}
	return true
}

// Compare an actual event with an expected event.
// Only compares the subset of fields relevant for testing.
func isEventf(t *testing.T, expected *armadaevents.EventSequence_Event, actual *armadaevents.EventSequence_Event, msg string, args ...interface{}) (ok bool) {
	defer func() {
		if !ok && msg != "" {
			t.Logf(msg, args...)
		}
	}()
	if ok = assert.IsType(t, expected.Event, actual.Event); !ok {
		return false
	}

	// If the expected event includes a jobId, the actual event must include the same jobId.
	expectedJobId, err := armadaevents.JobIdFromEvent(expected)
	if err == nil {
		actualJobId, err := armadaevents.JobIdFromEvent(actual)
		if ok = assert.NoError(t, err); !ok {
			return false
		}
		if ok = assert.Equal(t, expectedJobId, actualJobId); !ok {
			return false
		}
	}

	switch expectedEvent := expected.Event.(type) {
	case *armadaevents.EventSequence_Event_SubmitJob:
		actualEvent, ok := actual.Event.(*armadaevents.EventSequence_Event_SubmitJob)
		if ok := assert.True(t, ok); !ok {
			return false
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
func receiveJobSetSequences(ctx context.Context, consumer pulsar.Consumer, queue string, jobSetName string, maxEvents int, timeout time.Duration) (sequences []*armadaevents.EventSequence, err error) {
	sequences = make([]*armadaevents.EventSequence, 0)
	numEvents := 0
	for numEvents < maxEvents {
		ctxWithTimeout, _ := context.WithTimeout(ctx, timeout)
		var msg pulsar.Message
		msg, err = consumer.Receive(ctxWithTimeout)
		if err == context.DeadlineExceeded {
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

// Create a job submit request with multiple pods, and an ingress and service for testing.
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

	// Connection to Pulsar. To check that the correct sequence of messages are produced.
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:              pulsarUrl,
		OperationTimeout: 5 * time.Second,
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
	err = consumer.SeekByTime(time.Now())
	if err != nil {
		return errors.WithStack(err)
	}

	return action(context.Background(), submitClient, producer, consumer)
}
