package client

import (
	"encoding/json"
	"fmt"
)

type Resource struct {
	Version APIVersion   `json:"apiVersion" yaml:"apiVersion"`
	Kind    ResourceKind `json:"kind" yaml:"kind"`
}

type ResourceKind string

const (
	ResourceKindQueue ResourceKind = "Queue"
)

func NewResourceKind(in string) (ResourceKind, error) {
	validValues := []ResourceKind{ResourceKindQueue}
	if in != string(ResourceKindQueue) {
		return "", fmt.Errorf("invalid kind: %s. Valid values: %v", in, validValues)
	}

	return ResourceKind(in), nil
}

func (kind *ResourceKind) UnmarshalJSON(data []byte) error {
	var temp string

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	k, err := NewResourceKind(temp)
	if err != nil {
		return err
	}

	*kind = k

	return nil
}

type APIVersion string

const (
	APIVersionV1 APIVersion = "armadaproject.io/v1beta1"
)

func NewAPIVersion(in string) (APIVersion, error) {
	validValues := []APIVersion{APIVersionV1}
	if in != string(APIVersionV1) {
		return "", fmt.Errorf("invalid version: %s. Valid values: %v", in, validValues)
	}

	return APIVersion(in), nil
}

func (version *APIVersion) UnmarshalJSON(data []byte) error {
	var temp string

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	v, err := NewAPIVersion(temp)
	if err != nil {
		return err
	}

	*version = v

	return nil
}
