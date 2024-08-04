package metrics

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var (
	baseTime = time.Now()

	baseRun = jobdb.
		MinimalRun(uuid.New(), baseTime.UnixNano()).
		WithPool("testPool").
		WithExecutor("testExecutor")

	baseJob = testfixtures.
		Test1Cpu16GiJob(testfixtures.TestQueue, testfixtures.PriorityClass0).
		WithSubmittedTime(baseTime.UnixNano())
)

func TestReportJobStateTransitions(t *testing.T) {
	baseTimePlusSeconds := func(numSeconds int) *time.Time {
		newTime := baseTime.Add(time.Duration(numSeconds) * time.Second)
		return &newTime
	}

	tests := map[string]struct {
		errorRegexes            []*regexp.Regexp
		trackedResourceNames    []v1.ResourceName
		jsts                    []jobdb.JobStateTransitions
		jobRunErrorsByRunId     map[uuid.UUID]*armadaevents.Error
		expectedJobStateSeconds map[[4]string]float64
	}{
		"Leased": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(baseRun.WithLeasedTime(baseTimePlusSeconds(60))),
					Leased: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "leased", "queued"}: 60,
			},
		},
		"Pending": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62))),
					Pending: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "pending", "leased"}: 2,
			},
		},
		"Running": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72))),
					Running: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "running", "pending"}: 10,
			},
		},
		"Succeeded": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithTerminatedTime(baseTimePlusSeconds(80))),
					Succeeded: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "succeeded", "running"}: 8,
			},
		},
		"Cancelled": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithTerminatedTime(baseTimePlusSeconds(80))),
					Cancelled: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "cancelled", "running"}: 8,
			},
		},
		"Failed": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithTerminatedTime(baseTimePlusSeconds(80))),
					Failed: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "failed", "running"}: 8,
			},
		},
		"Preempted": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithPreemptedTime(baseTimePlusSeconds(80))),
					Preempted: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "preempted", "running"}: 8,
			},
		},
		"Multiple transitions": {
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(1)).
								WithPendingTime(baseTimePlusSeconds(3)).
								WithRunningTime(baseTimePlusSeconds(6)).
								WithTerminatedTime(baseTimePlusSeconds(10))),
					Leased:    true,
					Pending:   true,
					Running:   true,
					Succeeded: true,
				},
			},
			expectedJobStateSeconds: map[[4]string]float64{
				{"testQueue", "testPool", "leased", "queued"}:     1,
				{"testQueue", "testPool", "pending", "leased"}:    2,
				{"testQueue", "testPool", "running", "pending"}:   3,
				{"testQueue", "testPool", "succeeded", "running"}: 4,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			metrics := newJobStateMetrics(tc.errorRegexes, tc.trackedResourceNames)
			metrics.UpdateJobStateTransitionMetrics(tc.jsts, tc.jobRunErrorsByRunId)
			for k, v := range tc.expectedJobStateSeconds {
				actualJobStateSeconds := testutil.ToFloat64(metrics.queueJobStateSeconds.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobStateSeconds for %s", strings.Join(k[:], ","))
			}
		})
	}
}
