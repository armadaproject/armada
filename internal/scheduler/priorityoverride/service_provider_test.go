package priorityoverride

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
		overrides          []*priorityoverride.PoolPriorityOverrides
		expectedOverrides  map[overrideKey]float64
		expectedNotPresent []overrideKey
	}{
		"No Overrides": {
			overrides: []*priorityoverride.PoolPriorityOverrides{},
			expectedNotPresent: []overrideKey{
				{pool: "testpool", queue: "testQueue"},
			},
		},
		"One Overrides, One Pool": {
			overrides: []*priorityoverride.PoolPriorityOverrides{
				{
					Pool: "testpool",
					Overrides: map[string]float64{
						"testqueue": 2.0,
					},
				},
			},
			expectedOverrides: map[overrideKey]float64{
				{pool: "testpool", queue: "testqueue"}: 2.0,
			},
			expectedNotPresent: []overrideKey{
				{pool: "otherpool", queue: "testqueue"},
				{pool: "testpool", queue: "otherqueue"},
			},
		},
		"Multiple Overrides, Multiple pools": {
			overrides: []*priorityoverride.PoolPriorityOverrides{
				{
					Pool: "testpool",
					Overrides: map[string]float64{
						"testqueue":  2.0,
						"testqueue2": 3.0,
					},
				},
				{
					Pool: "testpool2",
					Overrides: map[string]float64{
						"testqueue":  4.0,
						"testqueue2": 5.0,
					},
				},
			},
			expectedOverrides: map[overrideKey]float64{
				{pool: "testpool", queue: "testqueue"}:   2.0,
				{pool: "testpool", queue: "testqueue2"}:  3.0,
				{pool: "testpool2", queue: "testqueue"}:  4.0,
				{pool: "testpool2", queue: "testqueue2"}: 5.0,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := armadacontext.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockClient := schedulermocks.NewMockPriorityOverrideServiceClient(ctrl)
			mockClient.EXPECT().
				GetPriorityOverrides(gomock.Any(), gomock.Any()).
				Return(&priorityoverride.PriorityOverrideResponse{PoolPriorityOverrides: tc.overrides}, nil)

			provider := NewServiceProvider(mockClient, 1*time.Millisecond)
			err := provider.fetchOverrides(ctx)
			require.NoError(t, err)

			for k, expectedOverride := range tc.expectedOverrides {
				override, ok, err := provider.Override(k.pool, k.queue)
				require.NoError(t, err)
				require.True(t, ok)
				assert.Equal(t, expectedOverride, override)
			}

			for _, k := range tc.expectedNotPresent {
				_, ok, err := provider.Override(k.pool, k.queue)
				require.NoError(t, err)
				require.False(t, ok)
			}
		})
	}
}

func TestReady(t *testing.T) {
	ctx := armadacontext.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := schedulermocks.NewMockPriorityOverrideServiceClient(ctrl)
	mockClient.EXPECT().
		GetPriorityOverrides(gomock.Any(), gomock.Any()).
		Return(&priorityoverride.PriorityOverrideResponse{PoolPriorityOverrides: []*priorityoverride.PoolPriorityOverrides{}}, nil)
	provider := NewServiceProvider(mockClient, 1*time.Millisecond)

	// Not ready before fetch
	assert.False(t, provider.Ready())
	_, _, err := provider.Override("pool", "queue")
	assert.Error(t, err)

	// Do the fetch
	err = provider.fetchOverrides(ctx)
	require.NoError(t, err)

	// Ready after Fetch
	assert.True(t, provider.Ready())
	_, _, err = provider.Override("pool", "queue")
	assert.NoError(t, err)
}
