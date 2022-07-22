package eventbenchmark

import (
	"encoding/json"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"
)

type Formatter func(input interface{}) ([]byte, error)

func JsonFormatter(input interface{}) ([]byte, error) {
	data, err := json.MarshalIndent(input, "", "\t")
	if err != nil {
		return nil, errors.WithMessage(err, "error marshalling event durations as json")
	}
	return data, nil
}

func YamlFormatter(input interface{}) ([]byte, error) {
	output, err := yaml.Marshal(input)
	if err != nil {
		return nil, errors.WithMessagef(err, "error marshalling data to yaml")
	}
	return output, nil
}

func in(data []string, elem string) bool {
	for _, e := range data {
		if e == elem {
			return true
		}
	}
	return false
}

func eventDurationToInt64(input []*EventDuration) []int64 {
	output := make([]int64, 0, len(input))
	for _, e := range input {
		output = append(output, int64(e.Duration))
	}
	return output
}
