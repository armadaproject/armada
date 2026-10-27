package startupmetrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestCollectionIssues(t *testing.T) {
	tests := map[string]struct {
		// pod defaults to one with an init container and an app container in its spec.
		pod *v1.Pod
		// modify adapts a complete breakdown, so each case says only what it is missing.
		modify   func(info *armadaevents.PodStartUpInfo)
		expected []string
	}{
		"a complete breakdown is missing nothing": {},
		"no pod instant determined": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.ScheduledAt = nil
				info.KubeletAcknowledgedAt = nil
				info.SandboxReadyAt = nil
				info.StartedAt = nil
			},
			expected: []string{fieldScheduledAt, fieldKubeletAcknowledgedAt, fieldSandboxReadyAt, fieldStartedAt},
		},
		"containers in the spec but none described": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers = nil
				info.Containers = nil
			},
			expected: []string{
				"init_containers (described 0 of the pod's 1)",
				"containers (described 0 of the pod's 1)",
			},
		},
		"fewer init containers described than the pod has": {
			pod:      podWithSpec(2, 1),
			expected: []string{"init_containers (described 1 of the pod's 2)"},
		},
		"no containers in the spec to describe": {
			pod: &v1.Pod{},
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers = nil
				info.Containers = nil
			},
		},
		"an instant missing from one init container": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers[0].StartedAt = nil
			},
			expected: []string{"init_containers (init has no start time)"},
		},
		"an instant missing from one app container": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.Containers[0].CreatedAt = nil
			},
			expected: []string{"containers (main has no creation time)"},
		},
		"an init container that did not finish": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers[0].FinishedAt = nil
			},
			expected: []string{"init_containers (init has no finish time)"},
		},
		"a sidecar is still running, so is not expected to have finished": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers[0].FinishedAt = nil
				info.InitContainers[0].Sidecar = true
			},
		},
		"no pull described at all": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.Containers[0].ImagePull = nil
			},
			expected: []string{"image_pull (main has no image pull described)"},
		},
		"a real pull carrying none of its details": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers[0].ImagePull = &armadaevents.ImagePullInfo{}
			},
			expected: []string{
				"image_pull_details (init pull has no duration)",
				"image_pull_details (init pull has no waiting time)",
				"image_pull_details (init pull has no size)",
			},
		},
		"a real pull missing one detail": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.InitContainers[0].ImagePull.SizeBytes = nil
			},
			expected: []string{"image_pull_details (init pull has no size)"},
		},
		"a cached pull is expected to carry a size, which is resolved best effort": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.Containers[0].ImagePull = &armadaevents.ImagePullInfo{Cached: true}
			},
			expected: []string{"cached_image_pull_size (main cached pull has no size)"},
		},
		"a cached pull is not expected to carry a duration or waiting time": {
			modify: func(info *armadaevents.PodStartUpInfo) {
				info.Containers[0].ImagePull = &armadaevents.ImagePullInfo{
					Cached:    true,
					SizeBytes: sizeBytes(734003200),
				}
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := tc.pod
			if pod == nil {
				pod = testPod("queue-1", "pool-1", "node-1")
			}
			info := fullyMeasuredInfo()
			if tc.modify != nil {
				tc.modify(info)
			}

			issues := collectionIssues(pod, info)

			assert.Equal(t, tc.expected, armadaslices.Map(issues, func(issue collectionIssue) string {
				return issue.String()
			}))
		})
	}
}

func TestIncompleteBreakdownMessage(t *testing.T) {
	sandboxReady := collectionIssue{field: fieldSandboxReadyAt}
	unfinishedInit := collectionIssue{field: fieldInitContainers, detail: "init has no finish time"}
	noPullSize := collectionIssue{field: fieldImagePullDetails, detail: "init pull has no size"}

	tests := map[string]struct {
		issues   []collectionIssue
		expected string
	}{
		"nothing missing": {},
		// Which is every pod on a cluster below Kubernetes 1.33.
		"only sandbox_ready_at, which the cluster may never report": {
			issues: []collectionIssue{sandboxReady},
		},
		"sandbox_ready_at alongside something worth knowing about": {
			issues:   []collectionIssue{sandboxReady, unfinishedInit},
			expected: "Start up breakdown of pod test-pod is incomplete: init_containers (init has no finish time)",
		},
		"several issues, each with its detail": {
			issues: []collectionIssue{unfinishedInit, noPullSize},
			expected: "Start up breakdown of pod test-pod is incomplete: " +
				"init_containers (init has no finish time), image_pull_details (init pull has no size)",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, incompleteBreakdownMessage("test-pod", tc.issues))
		})
	}
}
