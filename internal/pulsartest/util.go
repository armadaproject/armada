package pulsartest

import (
	"bytes"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/hashicorp/go-multierror"
	apimachineryYaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/G-Research/armada/pkg/armadaevents"
)

// UnmarshalEventSubmission unmarshalls bytes into an EventSequence
func UnmarshalEventSubmission(yamlBytes []byte, es *armadaevents.EventSequence) error {
	var result *multierror.Error
	successExpectedEvents := false
	successEverythingElse := false
	yamlSpecSeparator := []byte("---")
	docs := bytes.Split(yamlBytes, yamlSpecSeparator)
	for _, docYamlBytes := range docs {

		// yaml.Unmarshal can unmarshal everything,
		// but leaves oneof fields empty (these are silently discarded).
		if err := apimachineryYaml.NewYAMLOrJSONDecoder(bytes.NewReader(yamlBytes), 128).Decode(es); err != nil {
			result = multierror.Append(result, err)
		} else {
			successEverythingElse = true
		}

		// YAMLToJSON + jsonpb.Unmarshaler can unmarshal oneof fields,
		// but can't unmarshal k8s pod specs.
		docJsonBytes, err := yaml.YAMLToJSON(docYamlBytes)
		if err != nil {
			result = multierror.Append(result, err)
			continue
		}
		unmarshaler := jsonpb.Unmarshaler{AllowUnknownFields: true}
		err = unmarshaler.Unmarshal(bytes.NewReader(docJsonBytes), es)
		if err != nil {
			result = multierror.Append(result, err)
		} else {
			successExpectedEvents = true
		}
	}
	if !successExpectedEvents || !successEverythingElse {
		return result.ErrorOrNil()
	}
	return nil
}
