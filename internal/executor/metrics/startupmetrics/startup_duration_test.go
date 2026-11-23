package startupmetrics

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestDeriveStartUpDurations(t *testing.T) {
	durationOf := func(count int) *time.Duration {
		return new(time.Duration(count) * time.Second)
	}
	// The pod is created at +0s and its app containers have started at +137s.
	infoWithInitContainers := func(initContainers ...*armadaevents.ContainerStartUpInfo) *armadaevents.PodStartUpInfo {
		info := fullyMeasuredInfo()
		info.InitContainers = initContainers
		return info
	}
	ran := func(name string, from, to int) *armadaevents.ContainerStartUpInfo {
		return &armadaevents.ContainerStartUpInfo{Name: name, StartedAt: baselineWithOffset(from), FinishedAt: baselineWithOffset(to)}
	}

	tests := map[string]struct {
		info     *armadaevents.PodStartUpInfo
		tracked  map[string]bool
		expected startUpDurations
	}{
		"nil info": {},
		"no init containers, so every variant is the total": {
			info:     infoWithInitContainers(),
			expected: startUpDurations{total: durationOf(137), withoutUntrackedInitContainers: durationOf(137), withoutInitContainers: durationOf(137)},
		},
		"an untracked init container": {
			info:     infoWithInitContainers(ran("user-init", 70, 111)),
			expected: startUpDurations{total: durationOf(137), withoutUntrackedInitContainers: durationOf(96), withoutInitContainers: durationOf(96)},
		},
		"a tracked init container": {
			info:     infoWithInitContainers(ran("preflight", 10, 30), ran("user-init", 70, 111)),
			tracked:  map[string]bool{"preflight": true},
			expected: startUpDurations{total: durationOf(137), withoutUntrackedInitContainers: durationOf(96), withoutInitContainers: durationOf(76)},
		},
		"a sidecar is not excluded": {
			info: infoWithInitContainers(&armadaevents.ContainerStartUpInfo{
				Name: "proxy", Sidecar: true, StartedAt: baselineWithOffset(70),
			}),
			expected: startUpDurations{total: durationOf(137), withoutUntrackedInitContainers: durationOf(137), withoutInitContainers: durationOf(137)},
		},
		"no total, so no variants": {
			info: func() *armadaevents.PodStartUpInfo {
				info := infoWithInitContainers(ran("user-init", 70, 111))
				info.StartedAt = nil
				return info
			}(),
		},
		"init containers that ran longer than the start up leave out the variants": {
			info:     infoWithInitContainers(ran("user-init", 0, 200)),
			expected: startUpDurations{total: durationOf(137)},
		},
		"a nil init container is ignored": {
			info:     infoWithInitContainers(nil, ran("user-init", 70, 111)),
			expected: startUpDurations{total: durationOf(137), withoutUntrackedInitContainers: durationOf(96), withoutInitContainers: durationOf(96)},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, deriveStartUpDurations(tc.info, tc.tracked))
		})
	}
}
