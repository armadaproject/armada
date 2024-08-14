package cmd

import (
	"fmt"
	"strings"
)

func queueNameValidation(queueName string) error {
	if queueName == "" {
		return fmt.Errorf("cannot provide empty queue name")
	}
	return nil
}

func labelSliceAsMap(labels []string) (map[string]string, error) {
	mapToReturn := make(map[string]string)
	for _, label := range labels {
		splitLabel := strings.Split(label, "=")
		if len(splitLabel) != 2 {
			return nil, fmt.Errorf("invalid label: %s", label)
		}
		mapToReturn[splitLabel[0]] = splitLabel[1]
	}
	return mapToReturn, nil
}
