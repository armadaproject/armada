package armadaevents

import (
	"testing"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
)

func TestPodStartUpInfo_IsEmpty(t *testing.T) {
	anInstant := &types.Timestamp{Seconds: 1790000000}

	tests := map[string]struct {
		info     *PodStartUpInfo
		expected bool
	}{
		"nil":                  {info: nil, expected: true},
		"no fields set":        {info: &PodStartUpInfo{}, expected: true},
		"started at":           {info: &PodStartUpInfo{StartedAt: anInstant}},
		"scheduled at":         {info: &PodStartUpInfo{ScheduledAt: anInstant}},
		"kubelet acknowledged": {info: &PodStartUpInfo{KubeletAcknowledgedAt: anInstant}},
		"sandbox ready":        {info: &PodStartUpInfo{SandboxReadyAt: anInstant}},
		"an init container":    {info: &PodStartUpInfo{InitContainers: []*ContainerStartUpInfo{{Name: "init"}}}},
		"an app container":     {info: &PodStartUpInfo{Containers: []*ContainerStartUpInfo{{Name: "main"}}}},
		"a configured step":    {info: &PodStartUpInfo{ExtraSteps: []*StartUpStep{{Name: "cni"}}}},
		// CreatedAt is the pod's own creationTimestamp, so it is set even for a pod that reported
		// nothing else. On its own it describes no start up at all.
		"only created at": {info: &PodStartUpInfo{CreatedAt: anInstant}, expected: true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.info.IsEmpty())
		})
	}
}
