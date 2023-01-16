package pulsartest

import (
	"bytes"

	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

// UnmarshalEventSubmission unmarshalls bytes into an EventSequence
func UnmarshalEventSubmission(yamlBytes []byte, es *armadaevents.EventSequence) error {
	return apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(es)
}
