package armadaevents

import (
	"bytes"
	"testing"
	time "time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"
)

func generateES() ([]byte, error) {
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

func TestEventSequenceYamlUnmarshal(t *testing.T) {
	eventSeqYaml, err := generateES()
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
