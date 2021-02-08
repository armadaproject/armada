package task

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type task struct {
	function    func()
	interval    time.Duration
	metricName  string
	stopChannel chan bool
}

// BackgroundTaskManager is not threadsafe, it should only be accessed from a single thread.
type BackgroundTaskManager struct {
	tasks         []*task
	metricsPrefix string
	wg            *sync.WaitGroup
}

func NewBackgroundTaskManager(metricsPrefix string) *BackgroundTaskManager {
	return &BackgroundTaskManager{
		tasks:         []*task{},
		metricsPrefix: metricsPrefix,
		wg:            &sync.WaitGroup{},
	}
}

func (m *BackgroundTaskManager) Register(backgroundTask func(), interval time.Duration, metricName string) {
	task := &task{
		function:    backgroundTask,
		interval:    interval,
		metricName:  metricName,
		stopChannel: make(chan bool),
	}
	m.startBackgroundTask(task)
	m.tasks = append(m.tasks, task)
}

func (m *BackgroundTaskManager) StopAll(timeout time.Duration) bool {
	m.stopTasks()
	return m.waitForShutdownCompletion(timeout)
}

func (m *BackgroundTaskManager) startBackgroundTask(task *task) {
	var taskDurationHistogram = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    m.metricsPrefix + task.metricName + "_latency_seconds",
			Help:    "Background loop " + task.metricName + " latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 15),
		})

	m.wg.Add(1)
	go func() {
		start := time.Now()
		task.function()
		duration := time.Since(start)
		taskDurationHistogram.Observe(duration.Seconds())

		for {
			select {
			case <-time.After(task.interval):
			case <-task.stopChannel:
				m.wg.Done()
				return
			}
			innerStart := time.Now()
			task.function()
			innerDuration := time.Since(innerStart)
			taskDurationHistogram.Observe(innerDuration.Seconds())
		}
	}()
}

func (m *BackgroundTaskManager) waitForShutdownCompletion(timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		m.wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func (m *BackgroundTaskManager) stopTasks() {
	for _, task := range m.tasks {
		task.stopChannel <- true
	}
}
