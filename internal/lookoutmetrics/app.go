package lookoutmetrics

import (
	"container/heap"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"time"

	armadaconfig "github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
	"github.com/armadaproject/armada/internal/lookoutv2/repository"
)

type App struct {
	// Parameters passed to the CLI by the user.
	Params Params
	// Out is used to write the output. Defaults to standard out,
	// but can be overridden in tests to make assertions on the applications's output.
	Out io.Writer
	// Source of randomness. Tests can use a mocked random source in order to provide
	// deterministic testing behaviour.
	Random io.Reader
}

// Params struct holds all user-customizable parameters.
// Using a single struct for all CLI commands ensures that all flags are distinct
// and that they can be provided either dynamically on a command line, or
// statically in a config file that's reused between command runs.
type Params struct {
	Postgres armadaconfig.PostgresConfig
}

// New instantiates an App with default parameters, including standard output
// and cryptographically secure random source.
func New() *App {
	return &App{
		Params: Params{},
		Out:    os.Stdout,
		Random: rand.Reader,
	}
}

// type Transition string

// const (
// 	ActiveFromQueued    Transition = "ActiveFromQueued"
// 	FailedFromActive    Transition = "FailedFromActive"
// 	PreemptedFromActive Transition = "PreemptedFromActive"
// )

// type Event struct {
// 	Job        *model.Job
// 	Transition Transition
// 	Time       time.Time
// }

// Jobs exit because:
// Succeeded
// Failed
// Preempted
// Cancelled

// Meaning I just need
// JobSubmittedEvent (new job)
// JobLeasedEvent (was previously queued)
// JobStartedEvent (was previously pending)
// JobFailedEvent (was previously running)
// JobPreemptedEvent (was previously running)
// JobCancelledEvent (was previously running)

// type Event interface {
// 	Time() time.Time
// 	JobId() string
// 	Queue() string
// }

type EventType int

const (
	JobSubmittedEvent EventType = iota
	JobLeasedEvent    EventType = iota
	JobRunningEvent   EventType = iota
	JobFailedEvent    EventType = iota
	JobCancelledEvent EventType = iota
	JobPreemptedEvent EventType = iota
)

// type JobSubmittedEvent struct {
// 	Time time.Time
// 	Job  *model.Job
// }

// type JobLeasedEvent struct {
// 	Time time.Time
// 	Job  *model.Job
// }

// type JobStartedEvent struct {
// 	Time time.Time
// 	Job  *model.Job
// }

// type JobCancelledEvent struct {
// 	Time time.Time
// 	Job  *model.Job
// }

// type JobFailedEvent struct {
// 	Time time.Time
// 	Job  *model.Job
// }

// type JobPreemptedEvent struct {
// 	Time time.Time
// 	Job  *model.Job
// }

type Event struct {
	Time time.Time
	Type EventType
	Job  *model.Job
	// Job exit code if this event indicates the job has exited.
	ExitCode int
	// Job error message if if a JobFailedEvent.
	Error string
	// If true, there will be no more events for this job.
	Terminal bool
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

type PriorityQueue []Event

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[j].Time.After(pq[i].Time)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(Event)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = Event{} // avoid memory leak
	item.index = -1    // for safety
	*pq = old[0 : n-1]
	return item
}

type JobTracker struct {
	jobsBatchSize      int
	getJobsRepo        repository.GetJobsRepository
	eventPriorityQueue PriorityQueue
	getJobsResult      *repository.GetJobsResult
	jobIdLowerBound    string
}

func NewJobTracker(jobsBatchSize int, getJobsRepo repository.GetJobsRepository) *JobTracker {
	return &JobTracker{
		jobsBatchSize:      jobsBatchSize,
		getJobsRepo:        getJobsRepo,
		eventPriorityQueue: make(PriorityQueue, 0),
	}
}

func (jt *JobTracker) Next(ctx context.Context) (Event, bool, error) {
	if jt.getJobsResult == nil {
		getJobsResult, err := jt.getNextJobsResult(ctx)
		if err != nil {
			return Event{}, false, err
		}
		jt.getJobsResult = getJobsResult
	}
	for {
		if jt.getJobsResult == nil {
			break
		}
		if len(jt.getJobsResult.Jobs) == 0 {
			break
		}
		if len(jt.eventPriorityQueue) > 0 {
			// TODO: Add buffer time here.
			if jt.getJobsResult.Jobs[0].Submitted.After(jt.eventPriorityQueue[0].Time) {
				break
			}
		}

		jt.addJobsResultEventsToPriorityQueue(jt.getJobsResult)
		getJobsResult, err := jt.getNextJobsResult(ctx)
		if err != nil {
			return Event{}, false, err
		}
		jt.getJobsResult = getJobsResult
	}
	if len(jt.eventPriorityQueue) == 0 {
		return Event{}, false, nil
	}
	return heap.Pop(&jt.eventPriorityQueue).(Event), true, nil
}

func (jt *JobTracker) getNextJobsResult(ctx context.Context) (*repository.GetJobsResult, error) {
	filter := &model.Filter{
		Field: "jobId",
		Match: model.MatchGreaterThan,
		Value: jt.jobIdLowerBound,
	}
	getJobsResult, err := jt.getJobsRepo.GetJobs(
		ctx,
		[]*model.Filter{filter},
		&model.Order{
			Direction: model.DirectionAsc,
			Field:     "jobId",
		},
		0,
		jt.jobsBatchSize,
	)
	if err != nil {
		return nil, err
	}
	if len(getJobsResult.Jobs) > 0 {
		jt.jobIdLowerBound = getJobsResult.Jobs[len(getJobsResult.Jobs)-1].JobId
	}
	return getJobsResult, nil
}

func (jt *JobTracker) addJobsResultEventsToPriorityQueue(getJobsResult *repository.GetJobsResult) {
	for _, job := range getJobsResult.Jobs {
		run := lastRunFromJob(job)
		if run == nil {
			// Skip jobs with zero runs.
			continue
		}
		jt.push(
			Event{
				Time: job.Submitted,
				Type: JobSubmittedEvent,
				Job:  job,
			},
		)
		if job.Cancelled != nil {
			jt.push(
				Event{
					Time:     *job.Cancelled,
					Type:     JobCancelledEvent,
					Job:      job,
					Terminal: true,
				},
			)
		}
		for i, run := range job.Runs {
			jt.push(
				Event{
					Time: run.Pending,
					Type: JobLeasedEvent,
					Job:  job,
				},
			)
			if run.Started != nil {
				jt.push(
					Event{
						Time: *run.Started,
						Type: JobRunningEvent,
						Job:  job,
					},
				)
			}
			if run.Finished != nil {
				terminal := i == len(job.Runs)-1
				switch run.JobRunState {
				case string(lookout.JobRunFailed):
					jt.push(
						Event{
							Time:     *run.Finished,
							Type:     JobFailedEvent,
							Job:      job,
							Terminal: terminal,
							// TODO: ExitCode and Error.
						},
					)
				case string(lookout.JobRunPreempted):
					jt.push(
						Event{
							Time:     *run.Finished,
							Type:     JobPreemptedEvent,
							Job:      job,
							Terminal: terminal,
						},
					)
				}
			}
		}
	}
}

func (jt *JobTracker) push(event Event) {
	heap.Push(&jt.eventPriorityQueue, event)
}

// func (jt *JobTracker) ProcessEventsUntilTime(time time.Time) {
// 	for len(jt.EventPriorityQueue) > 0 {
// 		event := jt.EventPriorityQueue[0]
// 		if event.Time.After(time) {
// 			return
// 		}
// 		jt.ProcessEvent(event)
// 		heap.Pop(&jt.EventPriorityQueue)
// 	}
// }

// func (jt *JobTracker) ProcessEvent(event Event) {
// 	switch event.Type {
// 	case JobSubmittedEvent:
// 		jt.ActiveJobs[event.Job.JobId] = event.Job
// 		fmt.Println(event.Job.JobId, "submitted at", event.Time)
// 	case JobLeasedEvent:
// 		fmt.Println(event.Job.JobId, "leased at", event.Time)
// 		fmt.Println(event.Job.Cpu, "allocated to", event.Job.Queue)
// 	case JobRunningEvent:
// 		fmt.Println(event.Job.JobId, "running at", event.Time)
// 	case JobFailedEvent:
// 		fmt.Println(event.Job.JobId, "failed at", event.Time)
// 	case JobCancelledEvent:
// 		fmt.Println(event.Job.JobId, "cancelled at", event.Time)
// 	case JobPreemptedEvent:
// 		fmt.Println(event.Job.JobId, "preempted at", event.Time)
// 	}
// 	jt.LastTransitionTimeByJobId[event.Job.JobId] = event.Time
// 	if event.Terminal {
// 		fmt.Println(event.Job.Cpu, "de-allocated from", event.Job.Queue)
// 		delete(jt.LastTransitionTimeByJobId, event.Job.JobId)
// 		delete(jt.ActiveJobs, event.Job.JobId)
// 	}
// }

func (app *App) Run() error {
	db, err := database.OpenPgxPool(app.Params.Postgres)
	if err != nil {
		return err
	}
	getJobsRepo := repository.NewSqlGetJobsRepository(db)
	jt := NewJobTracker(10, getJobsRepo)
	ctx := context.Background()
	for {
		event, ok, err := jt.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Println("no more events; sleeping for 1 second")
			time.Sleep(time.Second)
			continue
		}
		switch event.Type {
		case JobSubmittedEvent:
			fmt.Println(event.Job.JobId, "submitted at", event.Time)
		case JobLeasedEvent:
			fmt.Println(event.Job.JobId, "leased at", event.Time)
			fmt.Println(event.Job.Cpu, "allocated to", event.Job.Queue)
		case JobRunningEvent:
			fmt.Println(event.Job.JobId, "running at", event.Time)
		case JobFailedEvent:
			fmt.Println(event.Job.JobId, "failed at", event.Time)
		case JobCancelledEvent:
			fmt.Println(event.Job.JobId, "cancelled at", event.Time)
		case JobPreemptedEvent:
			fmt.Println(event.Job.JobId, "preempted at", event.Time)
		}
		if event.Terminal {
			fmt.Println(event.Job.Cpu, "de-allocated from", event.Job.Queue)
		}
	}
	// jobIdLowerBound := ""
	// for {
	// 	getJobsResult, err := app.GetBatch(ctx, jobIdLowerBound, 10, getJobsRepo)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if len(getJobsResult.Jobs) == 0 {
	// 		fmt.Println("no more events; sleeping for 1 second")
	// 		time.Sleep(time.Second)
	// 		continue
	// 	}
	// 	timeLowerBound := getJobsResult.Jobs[0].Submitted
	// 	jt.ProcessEventsUntilTime(timeLowerBound)
	// 	fmt.Printf("Got %d (%d) jobs\n", len(getJobsResult.Jobs), getJobsResult.Count)
	// 	for _, job := range getJobsResult.Jobs {
	// 		run := lastRunFromJob(job)
	// 		if run == nil {
	// 			// Skip jobs with zero runs.
	// 			continue
	// 		}
	// 		jt.Push(
	// 			Event{
	// 				Time: job.Submitted,
	// 				Type: JobSubmittedEvent,
	// 				Job:  job,
	// 			},
	// 		)
	// 		if job.Cancelled != nil {
	// 			jt.Push(
	// 				Event{
	// 					Time:     *job.Cancelled,
	// 					Type:     JobCancelledEvent,
	// 					Job:      job,
	// 					Terminal: true,
	// 				},
	// 			)
	// 		}
	// 		for i, run := range job.Runs {
	// 			jt.Push(
	// 				Event{
	// 					Time: run.Pending,
	// 					Type: JobLeasedEvent,
	// 					Job:  job,
	// 				},
	// 			)
	// 			if run.Started != nil {
	// 				jt.Push(
	// 					Event{
	// 						Time: *run.Started,
	// 						Type: JobRunningEvent,
	// 						Job:  job,
	// 					},
	// 				)
	// 			}
	// 			if run.Finished != nil {
	// 				terminal := i == len(job.Runs)-1
	// 				switch run.JobRunState {
	// 				case string(lookout.JobRunFailed):
	// 					jt.Push(
	// 						Event{
	// 							Time:     *run.Finished,
	// 							Type:     JobFailedEvent,
	// 							Job:      job,
	// 							Terminal: terminal,
	// 							// TODO: ExitCode and Error.
	// 						},
	// 					)
	// 				case string(lookout.JobRunPreempted):
	// 					jt.Push(
	// 						Event{
	// 							Time:     *run.Finished,
	// 							Type:     JobPreemptedEvent,
	// 							Job:      job,
	// 							Terminal: terminal,
	// 						},
	// 					)
	// 				}
	// 			}
	// 		}
	// 		jobIdLowerBound = job.JobId
	// 	}
	// }
}

// func (app *App) GetBatch(ctx context.Context, jobIdLowerBound string, jobsBatchSize int, getJobsRepo repository.GetJobsRepository) (*repository.GetJobsResult, error) {
// 	filter := &model.Filter{
// 		Field: "jobId",
// 		Match: model.MatchGreaterThan,
// 		Value: jobIdLowerBound,
// 	}
// 	getJobsResult, err := getJobsRepo.GetJobs(
// 		ctx,
// 		[]*model.Filter{filter},
// 		&model.Order{
// 			Direction: model.DirectionAsc,
// 			Field:     "jobId",
// 		},
// 		0,
// 		jobsBatchSize,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return getJobsResult, nil
// }

func lastRunFromJob(job *model.Job) *model.Run {
	if job == nil || len(job.Runs) == 0 {
		return nil
	}
	return job.Runs[len(job.Runs)-1]
}

func resourcesFromJob(job *model.Job) (int64, int64, int64, int64) {
	return job.Cpu, job.Memory, job.Gpu, job.EphemeralStorage
}
