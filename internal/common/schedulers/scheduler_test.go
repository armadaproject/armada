package schedulers

import (
	"github.com/armadaproject/armada/internal/common/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSchedulerFromMsg(t *testing.T) {
	tests := map[string]struct {
		propertyValue     string
		expectedScheduler Scheduler
	}{
		"Legacy": {
			propertyValue:     LegacySchedulerAttribute,
			expectedScheduler: Legacy,
		},
		"Pulsar": {
			propertyValue:     PulsarSchedulerAttribute,
			expectedScheduler: Pulsar,
		},
		"All": {
			propertyValue:     AllSchedulersAttribute,
			expectedScheduler: All,
		},
		"empty": {
			propertyValue:     "",
			expectedScheduler: Legacy,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			msg := mocks.NewMockMessage(ctrl)
			msg.EXPECT().Properties().Return(map[string]string{
				PropertyName: tc.propertyValue,
			}).Times(1)
			result := SchedulerFromMsg(msg)
			assert.Equal(t, tc.expectedScheduler, result)
		})
	}
}
