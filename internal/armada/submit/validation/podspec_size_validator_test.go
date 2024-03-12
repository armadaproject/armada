package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
)

func TestPodSpecSizeValidator(t *testing.T) {
	defaultPodSpec := &v1.PodSpec{
		Volumes: []v1.Volume{
			{
				Name: "foo",
			},
		},
	}

	defaultPodSpecSize := uint(len(protoutil.MustMarshall(defaultPodSpec)))

	tests := map[string]struct {
		req            *api.JobSubmitRequestItem
		expectSuccess  bool
		maxPodSpecSize uint
	}{
		"valid podspec in podspec": {
			req:            &api.JobSubmitRequestItem{PodSpec: defaultPodSpec},
			maxPodSpecSize: defaultPodSpecSize,
			expectSuccess:  true,
		},
		"valid podspec in podspecs": {
			req:            &api.JobSubmitRequestItem{PodSpecs: []*v1.PodSpec{defaultPodSpec}},
			maxPodSpecSize: defaultPodSpecSize,
			expectSuccess:  true,
		},
		"invalid podspec in podspec": {
			req:            &api.JobSubmitRequestItem{PodSpec: defaultPodSpec},
			maxPodSpecSize: defaultPodSpecSize - 1,
			expectSuccess:  false,
		},
		"invalid podspec in podspecs": {
			req:            &api.JobSubmitRequestItem{PodSpecs: []*v1.PodSpec{defaultPodSpec}},
			maxPodSpecSize: defaultPodSpecSize - 1,
			expectSuccess:  false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := podSpecSizeValidator{maxSize: tc.maxPodSpecSize}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
