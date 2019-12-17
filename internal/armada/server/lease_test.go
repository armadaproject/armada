package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/api"
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
