package utils

import (
	"errors"
	"fmt"
	"strings"

	"github.com/armadaproject/armada/pkg/api"
)

func QueueNameValidation(queueName string) error {
	if queueName == "" {
		return fmt.Errorf("cannot provide empty queue name")
	}
	return nil
}

func LabelSliceAsMap(labels []string) (map[string]string, error) {
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

type ActiveJobState string

const (
	UNKNOWN ActiveJobState = "unknown"
	QUEUED  ActiveJobState = "queued"
	LEASED  ActiveJobState = "leased"
	PENDING ActiveJobState = "pending"
	RUNNING ActiveJobState = "running"
)

func ActiveJobStateFromString(v string) (ActiveJobState, error) {
	switch v {
	case "queued":
		return QUEUED, nil
	case "leased":
		return LEASED, nil
	case "pending":
		return PENDING, nil
	case "running":
		return RUNNING, nil
	default:
		return UNKNOWN, errors.New(`must be one of "queued", "leased", "pending", "running"`)
	}
}

func ApiJobStateFromActiveJobState(s ActiveJobState) api.JobState {
	switch s {
	case QUEUED:
		return api.JobState_QUEUED
	case LEASED:
		return api.JobState_LEASED
	case PENDING:
		return api.JobState_PENDING
	case RUNNING:
		return api.JobState_RUNNING
	case UNKNOWN:
		return api.JobState_UNKNOWN
	default:
		return api.JobState_UNKNOWN
	}
}

func (s *ActiveJobState) String() string {
	return string(*s)
}

func (s *ActiveJobState) Set(v string) error {
	switch v {
	case "queued", "leased", "pending", "running":
		*s = ActiveJobState(v)
		return nil
	default:
		return errors.New(`must be one of "queued", "leased", "pending", "running"`)
	}
}

func (s *ActiveJobState) Type() string {
	return "ActiveJobState"
}
