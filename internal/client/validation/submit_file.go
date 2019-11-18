package validation

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/instrumenta/kubeval/kubeval"
	log "github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/client/util"
)

type rawJobSubmitFile struct {
	Jobs []*rawJobRequest
}

type rawJobRequest struct {
	PodSpec *json.RawMessage `json:"PodSpec,omitempty"`
}

type rawKubernetesType struct {
	metav1.TypeMeta `json:",inline"`
	Spec            *json.RawMessage `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

func rawPod(spec *json.RawMessage) *rawKubernetesType {
	return &rawKubernetesType{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		Spec: spec,
	}
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

	noSchema := false
	for i, job := range submitFile.Jobs {
		rawPod := rawPod(job.PodSpec)
		result, err := validate(rawPod)

		if !result[0].ValidatedAgainstSchema {
			noSchema = true
		}

		if err != nil {
			return false, err
		}

		if len(result[0].Errors) > 0 {
			return false, fmt.Errorf("Validation error in job[%d]: %s", i, result[0].Errors[0].Description())
		}
	}

	if noSchema {
		log.Error("Unable to validate jobs. You must be offline as pod schemas cannot be retrieved to perform validation")
	}

	return true, nil
}

func validate(value *rawKubernetesType) ([]kubeval.ValidationResult, error) {
	output, _ := json.Marshal(value)
	config := kubeval.NewDefaultConfig()
	config.Strict = true
	config.IgnoreMissingSchemas = true
	return kubeval.Validate(output, config)
}
