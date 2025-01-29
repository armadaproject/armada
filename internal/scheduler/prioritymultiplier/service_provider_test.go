package prioritymultiplier

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/pkg/priorityoverride"
)

func TestFetch(t *testing.T) {
	tests := map[string]struct {
		multipliers         []*priorityoverride.PoolPriorityMultipliers
		expectedMultipliers map[multiplierKey]float64
	}{
		"No Multipliers": {
			multipliers: []*priorityoverride.PoolPriorityMultipliers{},
			expectedMultipliers: map[multiplierKey]float64{
				multiplierKey{pool: "testpool", queue: "testQueue"}: 1.0,
			},
		},
		"One Multiplier, One Pool": {
			multipliers: []*priorityoverride.PoolPriorityMultipliers{
				{
					Pool: "testpool",
					Multipliers: map[string]float64{
						"testqueue": 2.0,
					},
				},
			},
			expectedMultipliers: map[multiplierKey]float64{
				multiplierKey{pool: "testpool", queue: "testqueue"}:  2.0,
				multiplierKey{pool: "otherpool", queue: "testqueue"}: 1.0,
				multiplierKey{pool: "testpool", queue: "otherqueue"}: 1.0,
			},
		},
		"Multiple Multipliers, Multiple pools": {
			multipliers: []*priorityoverride.PoolPriorityMultipliers{
				{
					Pool: "testpool",
					Multipliers: map[string]float64{
						"testqueue":  2.0,
						"testqueue2": 3.0,
					},
				},
				{
					Pool: "testpool2",
					Multipliers: map[string]float64{
						"testqueue":  4.0,
						"testqueue2": 5.0,
					},
				},
			},
			expectedMultipliers: map[multiplierKey]float64{
				multiplierKey{pool: "testpool", queue: "testqueue"}:   2.0,
				multiplierKey{pool: "testpool", queue: "testqueue2"}:  3.0,
				multiplierKey{pool: "testpool2", queue: "testqueue"}:  4.0,
				multiplierKey{pool: "testpool2", queue: "testqueue2"}: 5.0,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := schedulermocks.NewMockPriorityMultiplierServiceClient(ctrl)
			mockClient.EXPECT().
				GetPriorityMultipliers(gomock.Any(), gomock.Any()).
				Return(&priorityoverride.PriorityMultiplierResponse{PoolPriorityMultipliers: tc.multipliers}, nil)

			provider := NewServiceProvider(mockClient, 1*time.Millisecond)
			err := provider.fetchMultipliers(ctx)
			require.NoError(t, err)

			for k, expectedMultiplier := range tc.expectedMultipliers {
				multiplier, err := provider.Multiplier(k.pool, k.queue)
				require.NoError(t, err)
				assert.Equal(t, expectedMultiplier, multiplier)
			}
		})
	}
}

func TestReady(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := schedulermocks.NewMockPriorityMultiplierServiceClient(ctrl)
	mockClient.EXPECT().
		GetPriorityMultipliers(gomock.Any(), gomock.Any()).
		Return(&priorityoverride.PriorityMultiplierResponse{PoolPriorityMultipliers: []*priorityoverride.PoolPriorityMultipliers{}}, nil)
	provider := NewServiceProvider(mockClient, 1*time.Millisecond)

	// Not ready before fetch
	assert.False(t, provider.Ready())
	_, err := provider.Multiplier("pool", "queue")
	assert.Error(t, err)

	// Do the fetch
	err = provider.fetchMultipliers(ctx)
	require.NoError(t, err)

	// Ready after Fetch
	assert.True(t, provider.Ready())
	_, err = provider.Multiplier("pool", "queue")
	assert.NoError(t, err)
}
