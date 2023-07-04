package lookoutmetrics

import (
	"container/heap"
	"context"
	"crypto/rand"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
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
	// Number of jobs to load from postgres at a time.
	JobsBatchSize int
	// Stop loading jobs from postgres when jobs returned from postgres
	// were submitted this amount of time later than the oldest local event.
	JobsTimeBuffer time.Duration
	// If non-empty, only jobs for this queue are retrieved from postgres.
	Queue string
	// Ignore any jobs with resource requests smaller than this.
	// Accepts "cpu", ephemeralStorage", "gpu", and "memory".
	ResourcesLowerBound map[string]int64
	// Ignore any jobs with resource requests greater than or equal to this.
	// Accepts "cpu", ephemeralStorage", "gpu", and "memory".
	ResourcesUpperBound map[string]int64
	// Only jobs with ids lexicographically greater than this are retrieved.
	JobIdLowerBound string
	// Name of the state file, containing, e.g., the timestamp of the most recent processed event.
	StateFileName string
	// Name of the postgres instance to connect to.
	PostgresInstanceName string
	// Map from postgres instance name to the config for that instance.
	PostgresConfigByInstanceName map[string]armadaconfig.PostgresConfig
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

type EventType int32

const (
	JobSubmittedEvent       EventType = iota
	JobLeasedEvent          EventType = iota
	JobRunningEvent         EventType = iota
	JobSucceededEvent       EventType = iota
	JobFailedEvent          EventType = iota
	JobPreemptedEvent       EventType = iota
	JobCancelledEvent       EventType = iota
	JobUnknownTerminalEvent EventType = iota
	JobFailedToStartEvent   EventType = iota
)

type Event struct {
	Time     time.Time
	Type     EventType
	Job      *model.Job
	RunIndex int
	// Job exit code if this event indicates the job has exited.
	ExitCode int32
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
	if pq[j].Time.Equal(pq[i].Time) {
		return pq[i].Job.JobId < pq[j].Job.JobId
	}
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
	jobsBatchSize             int
	jobsTimeBuffer            time.Duration
	queue                     string
	resourcesLowerBound       map[string]int64
	resourcesUpperBound       map[string]int64
	getJobsRepo               repository.GetJobsRepository
	getJobRunErrorRepo        repository.GetJobRunErrorRepository
	eventPriorityQueue        PriorityQueue
	getJobsResult             *repository.GetJobsResult
	jobIdLowerBound           string
	submitTimeOfMostRecentJob time.Time
	noMoreJobs                bool
}

func NewJobTracker(jobsBatchSize int, jobsTimeBuffer time.Duration, getJobsRepo repository.GetJobsRepository, getJobRunErrorRepo repository.GetJobRunErrorRepository) *JobTracker {
	return &JobTracker{
		jobsBatchSize:      jobsBatchSize,
		jobsTimeBuffer:     jobsTimeBuffer,
		getJobsRepo:        getJobsRepo,
		eventPriorityQueue: make(PriorityQueue, 0),
	}
}

func (jt *JobTracker) AddQueueFilter(queue string) {
	jt.queue = queue
}

func (jt *JobTracker) SetJobIdLowerBound(jobId string) {
	jt.jobIdLowerBound = jobId
}

func (jt *JobTracker) SetResourcesLowerBound(resourcesLowerBound map[string]int64) {
	jt.resourcesLowerBound = resourcesLowerBound
}

func (jt *JobTracker) SetResourcesUpperBound(resourcesUpperBound map[string]int64) {
	jt.resourcesUpperBound = resourcesUpperBound
}

func (jt *JobTracker) Next(ctx context.Context) (Event, bool, error) {
	for !jt.noMoreJobs && (len(jt.eventPriorityQueue) == 0 ||
		(len(jt.eventPriorityQueue) >= 0 && jt.submitTimeOfMostRecentJob.Sub(jt.eventPriorityQueue[0].Time) < jt.jobsTimeBuffer)) {
		tjobs := time.Now()
		getJobsResult, err := jt.getNextJobsResult(ctx)
		if err != nil {
			return Event{}, false, err
		}
		djobs := time.Since(tjobs)

		// Get errors for run with a non-zero exit code.
		// These must be retrieved separately.
		terrs := time.Now()
		var errorByJobRunId map[string]string
		if jt.getJobRunErrorRepo != nil {
			if errorByJobRunId, err = jt.errorsByRunIdFromGetJobsResult(ctx, getJobsResult); err != nil {
				return Event{}, false, err
			}
		}
		if len(errorByJobRunId) != 0 {
			fmt.Printf("// downloaded %d errors in %s\n", len(errorByJobRunId), time.Since(terrs))
		}

		jt.addJobsResultEventsToPriorityQueue(getJobsResult, errorByJobRunId)
		fmt.Printf("// downloaded %d jobs in %s; there are now %d events in the queue\n", len(getJobsResult.Jobs), djobs, len(jt.eventPriorityQueue))
	}
	if len(jt.eventPriorityQueue) == 0 {
		return Event{}, false, nil
	}
	return heap.Pop(&jt.eventPriorityQueue).(Event), true, nil
}

func (jt *JobTracker) getNextJobsResult(ctx context.Context) (*repository.GetJobsResult, error) {
	filters := []*model.Filter{
		{
			Field: "jobId",
			Match: model.MatchGreaterThan,
			Value: jt.jobIdLowerBound,
		},
	}
	if jt.queue != "" {
		filters = append(
			filters,
			&model.Filter{
				Field: "queue",
				Match: model.MatchExact,
				Value: jt.queue,
			},
		)
	}
	for t, q := range jt.resourcesLowerBound {
		filters = append(
			filters,
			&model.Filter{
				Field: t,
				Match: model.MatchGreaterThanOrEqualTo,
				Value: q,
			},
		)
	}
	for t, q := range jt.resourcesUpperBound {
		filters = append(
			filters,
			&model.Filter{
				Field: t,
				Match: model.MatchLessThanOrEqualTo,
				Value: q,
			},
		)
	}
	getJobsResult, err := jt.getJobsRepo.GetJobs(
		ctx,
		filters,
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
	} else {
		jt.noMoreJobs = true
	}
	return getJobsResult, nil
}

func (jt *JobTracker) errorsByRunIdFromGetJobsResult(ctx context.Context, getJobsResult *repository.GetJobsResult) (map[string]string, error) {
	errorByRunId := make(map[string]string)
	for _, job := range getJobsResult.Jobs {
		for _, run := range job.Runs {
			if run.ExitCode == nil || *run.ExitCode == 0 {
				continue
			}
			if runError, err := jt.getJobRunErrorRepo.GetJobRunError(ctx, run.RunId); err != nil {
				return nil, err
			} else if runError != "" {
				errorByRunId[run.RunId] = runError
			}
		}
	}
	return errorByRunId, nil
}

func (jt *JobTracker) addJobsResultEventsToPriorityQueue(getJobsResult *repository.GetJobsResult, errorByJobRunId map[string]string) {
	for _, job := range getJobsResult.Jobs {
		// Only consider jobs with a run with non-nil finished time or jobs in a terminal state with runs,
		// The second part is to work around a bug where the finished time of runs of preempted jobs is not set.
		jobHasFinishedRun := hasFinishedRun(job)
		jobHasFinishedRun = jobHasFinishedRun || (len(job.Runs) > 0 && jobIsInTerminalState(job))
		if !jobHasFinishedRun {
			continue
		}

		// Job submission.
		jt.push(
			Event{
				Time: job.Submitted,
				Type: JobSubmittedEvent,
				Job:  job,
			},
		)
		if jt.submitTimeOfMostRecentJob.Compare(job.Submitted) == -1 {
			jt.submitTimeOfMostRecentJob = job.Submitted
		}

		// Job runs.
		var previousRunFinishedTime time.Time
		slices.SortFunc(job.Runs, func(a, b *model.Run) bool {
			return a.Pending.Compare(b.Pending) != 1
		})
		for i, run := range job.Runs {
			finished := run.Finished
			if finished == nil && jobIsInTerminalState(job) {
				// Due to a bug, the finish time of preempted runs is not set.
				// However, job.LastTransitionTime does have the right timestamp.
				finished = &job.LastTransitionTime
			}
			if finished == nil {
				fmt.Printf("// skipping run with no finish time for job %s\n", job.JobId)
				// Ignore runs that haven't finished.
				continue
			}

			// Move pending to after submitted and the finish time of the previous run if events were recorded out of order.
			if job.Submitted.Compare(run.Pending) != -1 {
				run.Pending = job.Submitted.Add(time.Second)
			}
			if previousRunFinishedTime.Compare(run.Pending) != -1 {
				run.Pending = previousRunFinishedTime.Add(time.Second)
			}
			jt.push(
				Event{
					Time:     run.Pending,
					Type:     JobLeasedEvent,
					Job:      job,
					RunIndex: i,
				},
			)
			terminal := i == len(job.Runs)-1
			if run.Started == nil {
				// Runs that never started.
				// Move finished to after pending if events were recorded out of order.
				if run.Pending.Compare(*finished) != -1 {
					t := run.Pending.Add(time.Second)
					finished = &t
				}
				jt.push(
					Event{
						Time:     *finished,
						Type:     JobFailedToStartEvent,
						Job:      job,
						RunIndex: i,
						Terminal: terminal,
					},
				)
			} else {
				// Runs that started and have finished.
				// Move started to after pending and finished to after started if events were recorded out of order.
				if run.Pending.Compare(*run.Started) != -1 {
					t := run.Pending.Add(time.Second)
					run.Started = &t
				}
				if run.Started.Compare(*finished) != -1 {
					t := run.Started.Add(time.Second)
					finished = &t
				}
				jt.push(
					Event{
						Time:     *run.Started,
						Type:     JobRunningEvent,
						Job:      job,
						RunIndex: i,
					},
				)

				var exitCode int32
				if run.ExitCode != nil {
					exitCode = *run.ExitCode
				}

				if job.State == string(lookout.JobCancelled) {
					jt.push(
						Event{
							Time:     *finished,
							Type:     JobCancelledEvent,
							Job:      job,
							RunIndex: i,
							Terminal: terminal,
							ExitCode: exitCode,
							Error:    errorByJobRunId[run.RunId],
						},
					)
				} else if job.State == string(lookout.JobPreempted) || run.JobRunState == string(lookout.JobRunPreempted) {
					jt.push(
						Event{
							Time:     *finished,
							Type:     JobPreemptedEvent,
							Job:      job,
							RunIndex: i,
							Terminal: terminal,
							ExitCode: exitCode,
							Error:    errorByJobRunId[run.RunId],
						},
					)
				} else if run.JobRunState == string(lookout.JobRunFailed) {
					jt.push(
						Event{
							Time:     *finished,
							Type:     JobFailedEvent,
							Job:      job,
							RunIndex: i,
							Terminal: terminal,
							ExitCode: exitCode,
							Error:    errorByJobRunId[run.RunId],
						},
					)
				} else if run.JobRunState == string(lookout.JobRunSucceeded) {
					jt.push(
						Event{
							Time:     *finished,
							Type:     JobSucceededEvent,
							Job:      job,
							RunIndex: i,
							Terminal: terminal,
							ExitCode: exitCode,
							Error:    errorByJobRunId[run.RunId],
						},
					)
				} else {
					jt.push(
						Event{
							Time:     *finished,
							Type:     JobUnknownTerminalEvent,
							Job:      job,
							RunIndex: i,
							Terminal: terminal,
							ExitCode: exitCode,
							Error:    errorByJobRunId[run.RunId],
						},
					)
				}
			}
			previousRunFinishedTime = *finished
		}
	}
}

func (jt *JobTracker) push(event Event) {
	heap.Push(&jt.eventPriorityQueue, event)
}

func (app *App) Run() error {
	if app.Params.PostgresInstanceName == "" {
		return errors.Errorf("no postgres instance name provided")
	}
	postgresConfig, ok := app.Params.PostgresConfigByInstanceName[app.Params.PostgresInstanceName]
	if !ok {
		return errors.Errorf("no config for postgres instance %s", app.Params.PostgresInstanceName)
	}

	// Capture SIGINT/SIGTERM to exit cleanly.
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	shouldRun := atomic.Bool{}
	shouldRun.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-c
		shouldRun.Store(false)
		cancel()
	}()

	db, err := database.OpenPgxPool(postgresConfig)
	if err != nil {
		return err
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.Ping(ctxWithTimeout); err != nil {
		return err
	}
	fmt.Println("// ping successful")
	getJobsRepo := repository.NewSqlGetJobsRepository(db)
	// getJobRunErrorRepo := repository.NewSqlGetJobRunErrorRepository(db, compress.NewZlibDecompressor())

	// Load state file from disk. Defer writing an updated one.
	state := LookoutMetricsState{}
	if app.Params.StateFileName != "" {
		if dat, err := os.ReadFile(app.Params.StateFileName); err == nil {
			if err := proto.Unmarshal(dat, &state); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if state.PreviousEventTypeByJobId == nil {
		state.PreviousEventTimeByJobId = make(map[string]time.Time, 1024)
	}
	if state.PreviousEventTypeByJobId == nil {
		state.PreviousEventTypeByJobId = make(map[string]int32, 1024)
	}
	defer func() {
		if app.Params.StateFileName == "" {
			return
		}
		dat, err := proto.Marshal(&state)
		if err != nil {
			fmt.Print("// failed to marshal state\n")
			return
		}
		tmpFileName := app.Params.StateFileName + ".tmp"
		if err := os.WriteFile(tmpFileName, dat, 0644); err != nil {
			fmt.Printf("// failed to write state file to temp file %s\n", tmpFileName)
		}
		if err := os.Rename(tmpFileName, app.Params.StateFileName); err != nil {
			fmt.Printf("// failed to move state file to %s\n", app.Params.StateFileName)
		}
	}()

	jobIdLowerBound := ""
	for jobId := range state.PreviousEventTypeByJobId {
		if jobId == "" || jobId < jobIdLowerBound {
			jobIdLowerBound = jobId
		}
	}
	if app.Params.JobIdLowerBound > jobIdLowerBound {
		jobIdLowerBound = app.Params.JobIdLowerBound
	}

	jt := NewJobTracker(app.Params.JobsBatchSize, app.Params.JobsTimeBuffer, getJobsRepo, nil)
	jt.AddQueueFilter(app.Params.Queue)
	jt.SetJobIdLowerBound(jobIdLowerBound)
	jt.SetResourcesLowerBound(app.Params.ResourcesLowerBound)
	jt.SetResourcesUpperBound(app.Params.ResourcesUpperBound)
	defer fmt.Printf(
		"// exiting; there are %d active jobs and %d unprocessed events\n",
		len(state.PreviousEventTypeByJobId), len(jt.eventPriorityQueue),
	)

	for shouldRun.Load() {
		event, ok, err := jt.Next(ctx)
		if err != nil {
			return err
		} else if !ok {
			// No more events.
			break
		}

		// Update state.
		jobId := event.Job.JobId
		previousEventType := state.PreviousEventTypeByJobId[jobId]
		previousEventTime, ok := state.PreviousEventTimeByJobId[jobId]
		if !ok && event.Type != JobSubmittedEvent {
			fmt.Printf("// skipping %d event for job %s at %s: no previous JobSubmittedEvent\n", event.Type, jobId, event.Time.String())
			continue
		}
		if !event.Terminal {
			state.PreviousEventTypeByJobId[jobId] = int32(event.Type)
			state.PreviousEventTimeByJobId[jobId] = event.Time
		} else {
			delete(state.PreviousEventTypeByJobId, jobId)
			delete(state.PreviousEventTimeByJobId, jobId)
		}

		// Skip events we've already processed.
		// E.g., we may receive already processed events after a re-start.
		if event.Time.Equal(state.TimeOfMostRecentEvent) && jobId <= state.JobIdOfMostRecentEvent {
			// Events are ordered by time first and jobId second.
			continue
		} else if event.Time.Before(state.TimeOfMostRecentEvent) {
			continue
		}
		state.JobIdOfMostRecentEvent = jobId
		state.TimeOfMostRecentEvent = event.Time

		// Write record.
		priorityClass := ""
		if event.Job.PriorityClass != nil {
			priorityClass = *event.Job.PriorityClass
		}
		numRuns := len(event.Job.Runs)

		timestamp, err := event.Time.MarshalText()
		if err != nil {
			return err
		}
		timeSinceLastEvent := event.Time.Sub(previousEventTime).Seconds()
		if event.Type == JobSubmittedEvent {
			// JobSubmittedEvent is always the first event so is considered to have 0 timeSinceLastEvent.
			timeSinceLastEvent = 0
		}
		if timeSinceLastEvent < 0 {
			fmt.Printf("// skipping %d event for job %s: negative timeSinceLastEvent\n", event.Type, jobId)
		}

		fmt.Printf(
			"%s, %s, %s, %s, %d, %d, %s, %d, %d, %f, %d, %d, %d, %d, %d\n",
			string(timestamp),
			event.Job.Queue,
			event.Job.JobSet,
			event.Job.JobId,
			event.RunIndex,
			numRuns,
			priorityClass,
			previousEventType,
			event.Type,
			timeSinceLastEvent,
			event.Job.Cpu, event.Job.Memory, event.Job.Gpu, event.Job.EphemeralStorage,
			event.ExitCode,
		)
	}
	return nil
}

func lastFinishedRunFromJob(job *model.Job) *model.Run {
	var rv *model.Run
	for _, run := range job.Runs {
		if run.Finished != nil {
			rv = run
		}
	}
	return rv
}

// hasFinishedRun returns true if the job has at least one finished run.
func hasFinishedRun(job *model.Job) bool {
	for _, run := range job.Runs {
		if run.Finished != nil {
			return true
		}
	}
	return false
}

func jobIsInTerminalState(job *model.Job) bool {
	if job.State == string(lookout.JobCancelled) {
		return true
	}
	if job.State == string(lookout.JobPreempted) {
		return true
	}
	if job.State == string(lookout.JobFailed) {
		return true
	}
	return false
}

func resourcesFromJob(job *model.Job) (int64, int64, int64, int64) {
	return job.Cpu, job.Memory, job.Gpu, job.EphemeralStorage
}
