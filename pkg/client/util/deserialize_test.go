package util

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/domain"
)

func TestBindJsonOrYaml_Yaml(t *testing.T) {
	submitFile := &domain.JobSubmitFile{}
	err := BindJsonOrYaml(filepath.Join("testdata", "jobs.yaml"), submitFile)
	assert.NoError(t, err)
}

func TestBindJsonOrYaml_Json(t *testing.T) {
	submitFile := &domain.JobSubmitFile{}
	err := BindJsonOrYaml(filepath.Join("testdata", "jobs.json"), submitFile)
	assert.NoError(t, err)
}

func TestBindJsonOrYaml_IngressType(t *testing.T) {
	submitFile := &domain.JobSubmitFile{}
	err := BindJsonOrYaml(filepath.Join("testdata", "jobs-ingress.yaml"), submitFile)
	assert.NoError(t, err)
	assert.Equal(t, submitFile.Jobs[0].Ingress[0].Type, api.IngressType_Http)
	assert.Equal(t, submitFile.Jobs[0].Ingress[1].Type, api.IngressType_Http)
}
