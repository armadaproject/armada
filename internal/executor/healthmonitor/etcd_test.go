package healthmonitor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/executor/configuration"
)

func TestGetInstanceCurrentFractionOfResourceInUse(t *testing.T) {
	healthChecker := makeEtcHealthMonitor()

	instance := makeValidEtcdInstanceMonitor(10, 100)

	result, err := healthChecker.getInstanceCurrentFractionOfResourceInUse(instance)
	assert.NoError(t, err)
	assert.Equal(t, result, 0.1)
}

func TestGetInstanceCurrentFractionOfResourceInUse_ErrorsWhenNil(t *testing.T) {
	healthChecker := makeEtcHealthMonitor()
	_, err := healthChecker.getInstanceCurrentFractionOfResourceInUse(nil)
	assert.Error(t, err)
}

func TestGetInstanceCurrentFractionOfResourceInUse_ErrorsWhenNeverScraped(t *testing.T) {
	healthChecker := makeEtcHealthMonitor()
	instance := &etcdInstanceMonitor{}
	_, err := healthChecker.getInstanceCurrentFractionOfResourceInUse(instance)
	assert.Error(t, err)
}

func TestGetInstanceCurrentFractionOfResourceInUse_ErrorsWhenMissingMetrics(t *testing.T) {
	healthChecker := makeEtcHealthMonitor()

	instance := &etcdInstanceMonitor{
		err:       nil,
		lastCheck: time.Now(),
	}

	// In use size metric missing
	instance.metrics = map[string]float64{
		etcdInUseSizeBytesMetricName: 10,
	}
	_, err := healthChecker.getInstanceCurrentFractionOfResourceInUse(instance)
	assert.Error(t, err)

	// Total size metric missing
	instance.metrics = map[string]float64{
		etcdInUseSizeBytesMetricName: 10,
	}
	_, err = healthChecker.getInstanceCurrentFractionOfResourceInUse(instance)
	assert.Error(t, err)
}

func TestGetInstanceCurrentFractionOfResourceInUse_ErrorsWhenLastScrapeIsTooOld(t *testing.T) {
	healthChecker := makeEtcHealthMonitor()
	instance := &etcdInstanceMonitor{
		lastCheck: time.Now().Add(time.Minute * -20),
	}
	_, err := healthChecker.getInstanceCurrentFractionOfResourceInUse(instance)
	assert.Error(t, err)
}

func TestIsWithinSoftHealthLimit(t *testing.T) {
	// Current usage is 10%, under 30% limit
	healthChecker := makeEtcHealthMonitorWithInstances(0.3, 0.5, makeValidEtcdInstanceMonitor(10, 100))
	assert.True(t, healthChecker.IsWithinSoftHealthLimit())

	// Current usage is 30%, at 30% limit
	healthChecker = makeEtcHealthMonitorWithInstances(0.3, 0.5, makeValidEtcdInstanceMonitor(30, 100))
	assert.False(t, healthChecker.IsWithinSoftHealthLimit())

	// Current usage is 60%, over 30% limit
	healthChecker = makeEtcHealthMonitorWithInstances(0.3, 0.5, makeValidEtcdInstanceMonitor(60, 100))
	assert.False(t, healthChecker.IsWithinSoftHealthLimit())
}

func TestIsWithinSoftHealthLimit_WithMinAvailable(t *testing.T) {
	// Current usage is 10%, under 30% limit
	healthChecker := makeEtcHealthMonitorWithInstances(
		0.3,
		0.5,
		makeValidEtcdInstanceMonitor(10, 100),
		makeInvalidEtcdInstanceMonitor(),
		makeInvalidEtcdInstanceMonitor(),
	)

	healthChecker.etcdConfiguration.MinimumAvailable = 0
	assert.True(t, healthChecker.IsWithinSoftHealthLimit())

	healthChecker.etcdConfiguration.MinimumAvailable = 1
	assert.True(t, healthChecker.IsWithinSoftHealthLimit())

	healthChecker.etcdConfiguration.MinimumAvailable = 2
	assert.False(t, healthChecker.IsWithinSoftHealthLimit())
}

func TestIsWithinSoftHealthLimit_TakesMaxOfAllInstances(t *testing.T) {
	// Max usage is 10%, under 30% limit
	healthChecker := makeEtcHealthMonitorWithInstances(0.3, 0.5, makeValidEtcdInstanceMonitor(10, 100), makeValidEtcdInstanceMonitor(20, 100))
	assert.True(t, healthChecker.IsWithinSoftHealthLimit())

	// Max usage is 50%, over 30% limit
	healthChecker = makeEtcHealthMonitorWithInstances(0.3, 0.5, makeValidEtcdInstanceMonitor(30, 100), makeValidEtcdInstanceMonitor(50, 100))
	assert.False(t, healthChecker.IsWithinSoftHealthLimit())
}

func TestIsWithinHardHealthLimit(t *testing.T) {
	// Current usage is 10%, under 50% limit
	healthChecker := makeEtcHealthMonitorWithInstances(0.5, 0.5, makeValidEtcdInstanceMonitor(10, 100))
	assert.True(t, healthChecker.IsWithinHardHealthLimit())

	// Current usage is 50%, at 50% limit
	healthChecker = makeEtcHealthMonitorWithInstances(0.5, 0.5, makeValidEtcdInstanceMonitor(50, 100))
	assert.False(t, healthChecker.IsWithinHardHealthLimit())

	// Current usage is 60%, over 50% limit
	healthChecker = makeEtcHealthMonitorWithInstances(0.5, 0.5, makeValidEtcdInstanceMonitor(60, 100))
	assert.False(t, healthChecker.IsWithinHardHealthLimit())
}

func makeValidEtcdInstanceMonitor(inUse, total float64) *etcdInstanceMonitor {
	return &etcdInstanceMonitor{
		err:       nil,
		lastCheck: time.Now(),
		metrics: map[string]float64{
			etcdTotalSizeBytesMetricName: total,
			etcdInUseSizeBytesMetricName: inUse,
		},
	}
}

func makeInvalidEtcdInstanceMonitor() *etcdInstanceMonitor {
	return nil
}

func makeEtcHealthMonitorWithInstances(softLimit, hardLimit float64, instanceMonitors ...*etcdInstanceMonitor) *EtcdHealthMonitor {
	monitor := makeEtcHealthMonitor()

	monitor.instances = map[string]*etcdInstanceMonitor{}
	for i, instance := range instanceMonitors {
		monitor.instances[fmt.Sprintf("instance-%d", i)] = instance
	}
	etcdConfig := configuration.EtcdConfiguration{
		FractionOfStorageInUseSoftLimit: softLimit,
		FractionOfStorageInUseHardLimit: hardLimit,
	}
	monitor.etcdConfiguration = etcdConfig
	return monitor
}

func makeEtcHealthMonitor() *EtcdHealthMonitor {
	return &EtcdHealthMonitor{
		MetricsMaxAge: time.Minute * 5,
	}
}
