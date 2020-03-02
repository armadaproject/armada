package scheduling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
)

func Test_matchRequirements(t *testing.T) {

	job := &api.Job{RequiredNodeLabels: map[string]string{"armada/region": "eu", "armada/zone": "1"}}

	assert.False(t, matchRequirements(job, &api.LeaseRequest{}))
	assert.False(t, matchRequirements(job, &api.LeaseRequest{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"armada/region": "eu"}},
		{Labels: map[string]string{"armada/zone": "2"}},
	}}))
	assert.False(t, matchRequirements(job, &api.LeaseRequest{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "2"}},
	}}))

	assert.True(t, matchRequirements(job, &api.LeaseRequest{AvailableLabels: []*api.NodeLabeling{
		{Labels: map[string]string{"x": "y"}},
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "1", "x": "y"}},
	}}))
}

