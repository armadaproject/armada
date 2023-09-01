package etcdhealth

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/healthmonitor"
	"github.com/armadaproject/armada/internal/common/metrics"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestEtcdReplicaHealthMonitor(t *testing.T) {
	mp := &metrics.ManualMetricsProvider{}
	hm := NewEtcdReplicaHealthMonitor("foo", 0.2, 0.3, time.Second, time.Microsecond, 1e-3, 1.1, 10, mp)

	// Initial call results in unavailable.
	ok, reason, err := hm.IsHealthy()
	assert.False(t, ok)
	assert.Equal(t, healthmonitor.UnavailableReason, reason)
	assert.NoError(t, err)

	// Start the metrics collection service.
	ctx, cancel := armadacontext.WithCancel(armadacontext.Background())
	defer cancel()
	g, ctx := armadacontext.ErrGroup(ctx)
	g.Go(func() error { return hm.Run(ctx, logrus.NewEntry(logrus.New())) })

	// Should still be unavailable due to missing metrics.
	hm.BlockUntilNextMetricsCollection(ctx)
	ok, reason, err = hm.IsHealthy()
	assert.False(t, ok)
	assert.Equal(t, healthmonitor.UnavailableReason, reason)
	assert.NoError(t, err)

	// Metrics indicate healthy.
	mp.WithMetrics(map[string]float64{
		etcdSizeInUseBytesMetricName: 2,
		etcdSizeBytesMetricName:      3,
		etcdCapacityBytesMetricName:  10,
	})
	hm.BlockUntilNextMetricsCollection(ctx)
	ok, reason, err = hm.IsHealthy()
	assert.True(t, ok)
	assert.Empty(t, reason)
	assert.NoError(t, err)

	// Size in use metric indicates unhealthy.
	mp.WithMetrics(map[string]float64{
		etcdSizeInUseBytesMetricName: 2.1,
		etcdSizeBytesMetricName:      3,
		etcdCapacityBytesMetricName:  10,
	})
	hm.BlockUntilNextMetricsCollection(ctx)
	ok, reason, err = hm.IsHealthy()
	assert.False(t, ok)
	assert.Equal(t, EtcdReplicaSizeInUseExceededReason, reason)
	assert.NoError(t, err)

	// Size metric indicates unhealthy.
	mp.WithMetrics(map[string]float64{
		etcdSizeInUseBytesMetricName: 2,
		etcdSizeBytesMetricName:      3.1,
		etcdCapacityBytesMetricName:  10,
	})
	hm.BlockUntilNextMetricsCollection(ctx)
	ok, reason, err = hm.IsHealthy()
	assert.False(t, ok)
	assert.Equal(t, EtcdReplicaSizeExceededReason, reason)
	assert.NoError(t, err)
}
