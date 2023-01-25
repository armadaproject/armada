package armadaevents

import (
	"bytes"
	"testing"
	time "time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	armadaQueueName = "queue-a"
	armadaUserId    = "test-user"
	userNamespace   = "test-user-ns"
)

var (
	jobSetId string = util.NewULID()
	clientId string = uuid.New().String()
)

func generateBasicEventSequence_Event() ([]byte, error) {
	now := time.Now()

	evSubmitJob := EventSequence_Event_SubmitJob{
		SubmitJob: &SubmitJob{
			JobId:           ProtoUuidFromUuid(uuid.New()),
			DeduplicationId: "job-1",
			Priority:        1,
			ObjectMeta:      nil,
			MainObject:      nil,
			Objects:         []*KubernetesObject{},
			Lifetime:        3600,
			AtMostOnce:      true,
			Preemptible:     false,
			ConcurrencySafe: false,
			Scheduler:       "",
		},
	}

	ese := &EventSequence_Event{Created: &now, Event: &evSubmitJob}

	eseYaml, err := yaml.Marshal(ese)
	if err != nil {
		return []byte{}, err
	}

	return eseYaml, nil
}

func generateFullES() ([]byte, error) {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")

	reqItem := &api.JobSubmitRequestItem{
		Namespace: userNamespace,
		Priority:  1,
		ClientId:  clientId,
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

	es := ExpectedSequenceFromRequestItem(armadaQueueName, armadaUserId, userNamespace,
		jobSetId, ProtoUuidFromUuid(uuid.New()), reqItem)

	esYaml, err := yaml.Marshal(es)
	if err != nil {
		return []byte{}, err
	}

	return esYaml, nil
}

// Verify a single EventSequence_Event object
func TestEventSequence_EventUnmarshal(t *testing.T) {
	eventSeqYaml, err := generateBasicEventSequence_Event()
	assert.Nil(t, err)
	assert.NotEmpty(t, eventSeqYaml)

	es := &EventSequence_Event{}

	err = apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(eventSeqYaml), 128).Decode(es)
	assert.Nil(t, err)
	assert.NotNil(t, es.Created)

	var eseSubmitJob *EventSequence_Event_SubmitJob = es.Event.(*EventSequence_Event_SubmitJob)
	assert.NotNil(t, eseSubmitJob.SubmitJob)
	assert.Equal(t, uint32(1), eseSubmitJob.SubmitJob.Priority)
	assert.Equal(t, "job-1", eseSubmitJob.SubmitJob.DeduplicationId)
}

// Verify a full EventSequence, which may contain a number of events
func TestEventSequenceUnmarshal(t *testing.T) {
	eventSeqYaml, err := generateFullES()
	assert.Nil(t, err)
	assert.NotEmpty(t, eventSeqYaml)

	es := &EventSequence{}

	err = apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(eventSeqYaml), 128).Decode(es)
	assert.Nil(t, err)
	assert.Equal(t, armadaQueueName, es.Queue)
	assert.Equal(t, armadaUserId, es.UserId)
	assert.Equal(t, jobSetId, es.JobSetName)
	assert.Len(t, es.Groups, 0)
	assert.Len(t, es.Events, 6)

	assert.NotNil(t, es.Events[0].Event)
	evSubmitJob, ok := es.Events[0].Event.(*EventSequence_Event_SubmitJob)
	assert.True(t, ok)

	assert.Equal(t, clientId, evSubmitJob.SubmitJob.DeduplicationId)
	assert.Equal(t, uint32(1), evSubmitJob.SubmitJob.Priority)
	assert.Equal(t, userNamespace, evSubmitJob.SubmitJob.ObjectMeta.Namespace)
	assert.Nil(t, evSubmitJob.SubmitJob.ObjectMeta.Annotations)
}
