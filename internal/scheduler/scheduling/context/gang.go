package context

import (
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

type GangSchedulingContext struct {
	Created time.Time
	Queue   string
	GangInfo
	JobSchedulingContexts     []*JobSchedulingContext
	TotalResourceRequests     internaltypes.ResourceList
	AllJobsEvicted            bool
	RequestsFloatingResources bool
}

func NewGangSchedulingContext(jctxs []*JobSchedulingContext) *GangSchedulingContext {
	allJobsEvicted := true
	totalResourceRequests := internaltypes.ResourceList{}
	requestsFloatingResources := false
	for _, jctx := range jctxs {
		allJobsEvicted = allJobsEvicted && jctx.IsEvicted
		totalResourceRequests = totalResourceRequests.Add(jctx.Job.AllResourceRequirements())
		if jctx.Job.RequestsFloatingResources() {
			requestsFloatingResources = true
		}
	}
	// Uniformity of the values that we pick off the first job in the gang was
	// checked when the jobs were submitted (e.g., in ValidateApiJobs).
	representative := jctxs[0]
	return &GangSchedulingContext{
		Created:                   time.Now(),
		Queue:                     representative.Job.Queue(),
		GangInfo:                  representative.GangInfo,
		JobSchedulingContexts:     jctxs,
		TotalResourceRequests:     totalResourceRequests,
		AllJobsEvicted:            allJobsEvicted,
		RequestsFloatingResources: requestsFloatingResources,
	}
}

// JobIds returns a sliced composed of the ids of the jobs that make up the gang.
func (gctx *GangSchedulingContext) JobIds() []string {
	rv := make([]string, len(gctx.JobSchedulingContexts))
	for i, jctx := range gctx.JobSchedulingContexts {
		rv[i] = jctx.JobId
	}
	return rv
}

// Cardinality returns the number of jobs in the gang.
func (gctx *GangSchedulingContext) Cardinality() int {
	return len(gctx.JobSchedulingContexts)
}

type GangSchedulingFit struct {
	// The number of jobs in the gang that were successfully scheduled.
	NumScheduled int
	// The mean PreemptedAtPriority among successfully scheduled pods in the gang.
	MeanPreemptedAtPriority float64
}

func (f GangSchedulingFit) Less(other GangSchedulingFit) bool {
	return f.NumScheduled < other.NumScheduled || f.NumScheduled == other.NumScheduled && f.MeanPreemptedAtPriority > other.MeanPreemptedAtPriority
}

func (gctx *GangSchedulingContext) Fit() GangSchedulingFit {
	f := GangSchedulingFit{}
	totalPreemptedAtPriority := int32(0)
	for _, jctx := range gctx.JobSchedulingContexts {
		pctx := jctx.PodSchedulingContext
		if !pctx.IsSuccessful() {
			continue
		}
		f.NumScheduled++
		totalPreemptedAtPriority += pctx.PreemptedAtPriority
	}
	if f.NumScheduled == 0 {
		f.MeanPreemptedAtPriority = float64(totalPreemptedAtPriority)
	} else {
		f.MeanPreemptedAtPriority = float64(totalPreemptedAtPriority) / float64(f.NumScheduled)
	}
	return f
}
