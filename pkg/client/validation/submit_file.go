package validation

import (
	"encoding/json"
	"errors"

	"github.com/armadaproject/armada/pkg/client/util"
)

type rawJobSubmitFile struct {
	Jobs []*rawJobRequest
}

type rawJobRequest struct {
	PodSpec *json.RawMessage `json:"PodSpec,omitempty"`
}

func ValidateSubmitFile(filePath string) (bool, error) {
	submitFile := &rawJobSubmitFile{}
	err := util.BindJsonOrYaml(filePath, submitFile)
	if err != nil {
		return false, err
	}

	if len(submitFile.Jobs) <= 0 {
		return false, errors.New("Warning: You have provided no jobs to submit.")
	}

	return true, nil
}
