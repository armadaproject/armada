package querier

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/logging"

	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/db"
	"github.com/armadaproject/armada/internal/broadside/jobspec"
	"github.com/armadaproject/armada/internal/broadside/metrics"
	"github.com/armadaproject/armada/internal/lookout/model"
)

const (
	numFilterCombinations        = 7
	submittedTimeRangeProportion = 0.05
)

var (
	orderFields = []string{
		"jobId",
		"jobSet",
		"submitted",
		"lastTransitionTime",
		"queue",
		"state",
	}

	orderDirections = []string{
		model.DirectionAsc,
		model.DirectionDesc,
	}

	priorityMatches = []string{
		model.MatchExact,
		model.MatchGreaterThan,
		model.MatchLessThan,
		model.MatchGreaterThanOrEqualTo,
		model.MatchLessThanOrEqualTo,
	}
)

// Querier executes queries against the database to simulate user query load.
type Querier struct {
	queryConfig  configuration.QueryConfig
	queueConfigs []configuration.QueueConfig
	database     db.Database
	metrics      *metrics.QuerierMetrics
	testStart    time.Time
	testDuration time.Duration
	queryWg      sync.WaitGroup // Tracks in-flight queries
}

// NewQuerier creates a new Querier with the given configuration.
func NewQuerier(
	queryConfig configuration.QueryConfig,
	queueConfigs []configuration.QueueConfig,
	database db.Database,
	metrics *metrics.QuerierMetrics,
	testStart time.Time,
	testDuration time.Duration,
) *Querier {
	return &Querier{
		queryConfig:  queryConfig,
		queueConfigs: queueConfigs,
		database:     database,
		metrics:      metrics,
		testStart:    testStart,
		testDuration: testDuration,
	}
}

// Run starts the query simulation. It executes queries at the configured rate.
// This method blocks until the context is cancelled.
func (q *Querier) Run(ctx context.Context) {
	if q.queryConfig.GetJobsQueriesPerHour <= 0 && q.queryConfig.GetJobGroupsQueriesPerHour <= 0 {
		logging.Info("GetJobsQueriesPerHour and GetJobGroupsQueriesPerHour are both 0, querier will not run")
		return
	}

	jobsQueriesPerSecond := float64(q.queryConfig.GetJobsQueriesPerHour) / 3600.0
	accumulatedJobsQueries := 0.0
	jobsQueryCounter := 0

	groupQueriesPerSecond := float64(q.queryConfig.GetJobGroupsQueriesPerHour) / 3600.0
	accumulatedGroupQueries := 0.0
	groupQueryCounter := 0

	secondTicker := time.NewTicker(time.Second)
	defer secondTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Wait for all in-flight queries to complete before returning
			q.queryWg.Wait()
			return

		case <-secondTicker.C:
			if q.queryConfig.GetJobsQueriesPerHour > 0 {
				accumulatedJobsQueries += jobsQueriesPerSecond
				toExecute := int(accumulatedJobsQueries)
				accumulatedJobsQueries -= float64(toExecute)

				for range toExecute {
					q.queryWg.Add(1)
					go func(counter int) {
						defer q.queryWg.Done()
						q.executeGetJobsQuery(ctx, counter)
					}(jobsQueryCounter)
					jobsQueryCounter++
				}
			}

			if q.queryConfig.GetJobGroupsQueriesPerHour > 0 {
				accumulatedGroupQueries += groupQueriesPerSecond
				toExecute := int(accumulatedGroupQueries)
				accumulatedGroupQueries -= float64(toExecute)

				for range toExecute {
					q.queryWg.Add(1)
					go func(counter int) {
						defer q.queryWg.Done()
						q.executeGetJobGroupsQuery(ctx, counter)
					}(groupQueryCounter)
					groupQueryCounter++
				}
			}
		}
	}
}

// Metrics returns the current querier metrics.
func (q *Querier) Metrics() *metrics.QuerierMetrics {
	return q.metrics
}

func (q *Querier) executeGetJobsQuery(ctx context.Context, queryIndex int) {
	filters, filterCombo := q.buildFilters(queryIndex)
	order := q.buildOrder(queryIndex)

	pageSize := q.queryConfig.GetJobsPageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	start := time.Now()
	jobs, err := q.database.GetJobs(&ctx, filters, true, order, 0, pageSize)
	duration := time.Since(start)

	if errors.Is(err, context.Canceled) {
		return
	}

	q.metrics.RecordQuery(metrics.QueryTypeGetJobs, filterCombo, duration, err)

	if err != nil {
		logging.WithError(err).WithFields(map[string]any{
			"queryIndex":  queryIndex,
			"filterCombo": filterCombo,
		}).Warn("GetJobs query failed")
		return
	}

	if len(jobs) == 0 {
		return
	}

	switch filterCombo {
	case metrics.FilterCombinationErroredTime:
		q.executeFollowUpQueriesForErroredJobs(ctx, jobs)
	case metrics.FilterCombinationNone:
		q.executeFollowUpQueriesForAllJobs(ctx, jobs)
	}
}

func (q *Querier) buildOrder(queryIndex int) *model.Order {
	numCombinations := len(orderFields) * len(orderDirections)
	combinationIndex := queryIndex % numCombinations

	fieldIndex := combinationIndex / len(orderDirections)
	directionIndex := combinationIndex % len(orderDirections)

	return &model.Order{
		Field:     orderFields[fieldIndex],
		Direction: orderDirections[directionIndex],
	}
}

func (q *Querier) buildFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	filterComboIndex := queryIndex % numFilterCombinations

	switch filterComboIndex {
	case 0:
		return q.buildNoFilters()
	case 1:
		return q.buildAnnotationFilters(queryIndex)
	case 2:
		return q.buildRunningQueueFilters(queryIndex)
	case 3:
		return q.buildQueuedJobSetFilters(queryIndex)
	case 4:
		return q.buildPriorityFilters(queryIndex)
	case 5:
		return q.buildErroredTimeFilters(queryIndex)
	case 6:
		return q.buildClusterNodeFilters(queryIndex)
	default:
		return q.buildNoFilters()
	}
}

func (q *Querier) buildNoFilters() ([]*model.Filter, metrics.FilterCombination) {
	return []*model.Filter{}, metrics.FilterCombinationNone
}

func (q *Querier) buildAnnotationFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	numAnnotations := len(jobspec.AnnotationConfigs)
	filterCount := (queryIndex % numAnnotations) + 1

	filters := make([]*model.Filter, filterCount)
	for i := 0; i < filterCount; i++ {
		annotationConfig := jobspec.AnnotationConfigs[i]
		valueIndex := queryIndex % annotationConfig.MaxUniqueValues
		filters[i] = &model.Filter{
			Field:        annotationConfig.Key,
			Match:        model.MatchExact,
			Value:        jobspec.CreateAnnotationValue(valueIndex),
			IsAnnotation: true,
		}
	}

	return filters, metrics.FilterCombinationAnnotations
}

func (q *Querier) buildRunningQueueFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	queueName := q.selectQueue(queryIndex)

	return []*model.Filter{
		{
			Field:        "queue",
			Match:        model.MatchAnyOf,
			Value:        []string{queueName},
			IsAnnotation: false,
		},
		{
			Field:        "state",
			Match:        model.MatchAnyOf,
			Value:        []string{jobspec.QueryStateRunning},
			IsAnnotation: false,
		},
	}, metrics.FilterCombinationRunningQueue
}

func (q *Querier) buildQueuedJobSetFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	queueIdx, jobSetName := q.selectQueueAndJobSet(queryIndex)
	if len(q.queueConfigs) == 0 {
		return []*model.Filter{}, metrics.FilterCombinationQueuedJobSet
	}
	queueName := q.queueConfigs[queueIdx].Name

	return []*model.Filter{
		{
			Field:        "queue",
			Match:        model.MatchAnyOf,
			Value:        []string{queueName},
			IsAnnotation: false,
		},
		{
			Field:        "jobSet",
			Match:        model.MatchExact,
			Value:        jobSetName,
			IsAnnotation: false,
		},
		{
			Field: "state",
			Match: model.MatchAnyOf,
			Value: []string{
				jobspec.QueryStateQueued,
				jobspec.QueryStateLeased,
				jobspec.QueryStatePending,
			},
			IsAnnotation: false,
		},
	}, metrics.FilterCombinationQueuedJobSet
}

func (q *Querier) buildPriorityFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	queueName := q.selectQueue(queryIndex)
	matchIndex := queryIndex % len(priorityMatches)
	priorityValue := (queryIndex % jobspec.PriorityValues) + 1

	return []*model.Filter{
		{
			Field:        "queue",
			Match:        model.MatchAnyOf,
			Value:        []string{queueName},
			IsAnnotation: false,
		},
		{
			Field:        "priority",
			Match:        priorityMatches[matchIndex],
			Value:        priorityValue,
			IsAnnotation: false,
		},
	}, metrics.FilterCombinationPriority
}

func (q *Querier) buildErroredTimeFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	timeRangeDuration := time.Duration(float64(q.testDuration) * submittedTimeRangeProportion)

	rangeStart := q.testStart
	rangeEnd := q.testStart.Add(timeRangeDuration)

	return []*model.Filter{
		{
			Field:        "state",
			Match:        model.MatchAnyOf,
			Value:        []string{jobspec.QueryStateFailed},
			IsAnnotation: false,
		},
		{
			Field:        "submitted",
			Match:        model.MatchGreaterThanOrEqualTo,
			Value:        rangeStart,
			IsAnnotation: false,
		},
		{
			Field:        "submitted",
			Match:        model.MatchLessThanOrEqualTo,
			Value:        rangeEnd,
			IsAnnotation: false,
		},
	}, metrics.FilterCombinationErroredTime
}

func (q *Querier) buildClusterNodeFilters(queryIndex int) ([]*model.Filter, metrics.FilterCombination) {
	clusterIndex := (queryIndex % jobspec.ClusterValues) + 1
	nodeIndex := (queryIndex % jobspec.NodeValuesPerCluster) + 1

	clusterName := jobspec.CreateClusterName(clusterIndex)
	nodeName := jobspec.CreateNodeName(clusterIndex, nodeIndex)

	return []*model.Filter{
		{
			Field:        "cluster",
			Match:        model.MatchExact,
			Value:        clusterName,
			IsAnnotation: false,
		},
		{
			Field:        "node",
			Match:        model.MatchExact,
			Value:        nodeName,
			IsAnnotation: false,
		},
	}, metrics.FilterCombinationClusterNode
}

func (q *Querier) selectQueue(queryIndex int) string {
	if len(q.queueConfigs) == 0 {
		return ""
	}
	queueIdx := queryIndex % len(q.queueConfigs)
	return q.queueConfigs[queueIdx].Name
}

func (q *Querier) selectQueueAndJobSet(queryIndex int) (int, string) {
	if len(q.queueConfigs) == 0 {
		return 0, ""
	}

	queueIdx := queryIndex % len(q.queueConfigs)
	queue := q.queueConfigs[queueIdx]

	if len(queue.JobSetConfig) == 0 {
		return queueIdx, ""
	}

	jobSetIdx := queryIndex % len(queue.JobSetConfig)
	return queueIdx, queue.JobSetConfig[jobSetIdx].Name
}

func (q *Querier) executeGetJobGroupsQuery(ctx context.Context, queryIndex int) {
	groupedField := q.buildGroupedField(queryIndex)
	aggregates := q.buildAggregates(queryIndex, groupedField)
	filters, filterCombo := q.buildFilters(queryIndex)
	order := q.buildGroupOrder(queryIndex, groupedField, aggregates)

	pageSize := q.queryConfig.GetJobGroupsPageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	start := time.Now()
	_, err := q.database.GetJobGroups(&ctx, filters, order, groupedField, aggregates, 0, pageSize)
	duration := time.Since(start)

	if errors.Is(err, context.Canceled) {
		return
	}

	q.metrics.RecordQuery(metrics.QueryTypeGetJobGroups, filterCombo, duration, err)

	if err != nil {
		logging.WithError(err).WithFields(map[string]any{
			"queryIndex":   queryIndex,
			"filterCombo":  filterCombo,
			"groupedField": groupedField.Field,
			"aggregates":   aggregates,
		}).Warn("GetJobGroups query failed")
	}
}

func (q *Querier) buildGroupedField(queryIndex int) *model.GroupedField {
	groupedFieldOptions := []struct {
		field        string
		isAnnotation bool
	}{
		{field: "queue", isAnnotation: false},
		{field: "namespace", isAnnotation: false},
		{field: "jobSet", isAnnotation: false},
		{field: "state", isAnnotation: false},
		{field: "cluster", isAnnotation: false},
		{field: "node", isAnnotation: false},
	}

	for _, annotationConfig := range jobspec.AnnotationConfigs {
		groupedFieldOptions = append(groupedFieldOptions, struct {
			field        string
			isAnnotation bool
		}{
			field:        annotationConfig.Key,
			isAnnotation: true,
		})
	}

	groupedFieldIndex := queryIndex % len(groupedFieldOptions)
	selectedGroupedField := groupedFieldOptions[groupedFieldIndex]

	groupedField := &model.GroupedField{
		Field:                       selectedGroupedField.field,
		IsAnnotation:                selectedGroupedField.isAnnotation,
		LastTransitionTimeAggregate: "",
	}

	return groupedField
}

func (q *Querier) buildAggregates(queryIndex int, groupedField *model.GroupedField) []string {
	availableAggregates := []string{"submitted", "lastTransitionTime", "state"}

	aggregateOptions := [][]string{{}}
	for _, aggregate := range availableAggregates {
		currentLen := len(aggregateOptions)
		for i := 0; i < currentLen; i++ {
			newCombo := make([]string, len(aggregateOptions[i]))
			copy(newCombo, aggregateOptions[i])
			newCombo = append(newCombo, aggregate)
			aggregateOptions = append(aggregateOptions, newCombo)
		}
	}

	groupedFieldOptionsCount := 6 + len(jobspec.AnnotationConfigs)
	aggregateIndex := (queryIndex / groupedFieldOptionsCount) % len(aggregateOptions)
	aggregates := aggregateOptions[aggregateIndex]

	if containsString(aggregates, "lastTransitionTime") {
		lttAggregateOptions := []string{model.AggregateLatest, model.AggregateEarliest, model.AggregateAverage}
		lttAggregateIndex := (queryIndex / (groupedFieldOptionsCount * len(aggregateOptions))) % len(lttAggregateOptions)
		groupedField.LastTransitionTimeAggregate = lttAggregateOptions[lttAggregateIndex]
	}

	return aggregates
}

func (q *Querier) buildGroupOrder(queryIndex int, groupedField *model.GroupedField, aggregates []string) *model.Order {
	validOrderFields := []string{"count"}
	validOrderFields = append(validOrderFields, aggregates...)
	if !containsString(validOrderFields, groupedField.Field) && !groupedField.IsAnnotation {
		validOrderFields = append(validOrderFields, groupedField.Field)
	}

	orderDirections := []string{model.DirectionAsc, model.DirectionDesc}
	numOrderCombinations := len(validOrderFields) * len(orderDirections)

	orderCombinationIndex := queryIndex % numOrderCombinations
	orderFieldIndex := orderCombinationIndex / len(orderDirections)
	orderDirectionIndex := orderCombinationIndex % len(orderDirections)

	return &model.Order{
		Field:     validOrderFields[orderFieldIndex],
		Direction: orderDirections[orderDirectionIndex],
	}
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func (q *Querier) executeFollowUpQueriesForErroredJobs(ctx context.Context, jobs []*model.Job) {
	debugMessageRate := q.queryConfig.GetJobRunDebugMessageQueriesPerHour
	runErrorRate := q.queryConfig.GetJobRunErrorQueriesPerHour

	if debugMessageRate <= 0 && runErrorRate <= 0 {
		return
	}

	erroredTimeFilterFrequency := float64(q.queryConfig.GetJobsQueriesPerHour) / float64(numFilterCombinations)

	debugMessageQueriesToExecute := 0
	if debugMessageRate > 0 && erroredTimeFilterFrequency > 0 {
		debugMessageQueriesPerErroredTimeQuery := float64(debugMessageRate) / erroredTimeFilterFrequency
		debugMessageQueriesToExecute = int(debugMessageQueriesPerErroredTimeQuery)
		if debugMessageQueriesPerErroredTimeQuery > float64(int(debugMessageQueriesPerErroredTimeQuery)) {
			debugMessageQueriesToExecute++
		}
	}

	runErrorQueriesToExecute := 0
	if runErrorRate > 0 && erroredTimeFilterFrequency > 0 {
		runErrorQueriesPerErroredTimeQuery := float64(runErrorRate) / erroredTimeFilterFrequency
		runErrorQueriesToExecute = int(runErrorQueriesPerErroredTimeQuery)
		if runErrorQueriesPerErroredTimeQuery > float64(int(runErrorQueriesPerErroredTimeQuery)) {
			runErrorQueriesToExecute++
		}
	}

	jobIndex := 0
	for i := 0; i < debugMessageQueriesToExecute || i < runErrorQueriesToExecute; i++ {
		if len(jobs) == 0 {
			break
		}

		job := jobs[jobIndex%len(jobs)]
		jobIndex++

		if job.LastActiveRunId == nil {
			continue
		}

		runID := *job.LastActiveRunId

		if i < debugMessageQueriesToExecute {
			q.queryWg.Add(1)
			go func(runID string) {
				defer q.queryWg.Done()
				q.executeGetJobRunDebugMessage(ctx, runID)
			}(runID)
		}

		if i < runErrorQueriesToExecute {
			q.queryWg.Add(1)
			go func(runID string) {
				defer q.queryWg.Done()
				q.executeGetJobRunError(ctx, runID)
			}(runID)
		}
	}
}

func (q *Querier) executeFollowUpQueriesForAllJobs(ctx context.Context, jobs []*model.Job) {
	jobSpecRate := q.queryConfig.GetJobSpecQueriesPerHour

	if jobSpecRate <= 0 {
		return
	}

	noFilterFrequency := float64(q.queryConfig.GetJobsQueriesPerHour) / float64(numFilterCombinations)

	jobSpecQueriesToExecute := 0
	if jobSpecRate > 0 && noFilterFrequency > 0 {
		jobSpecQueriesPerNoFilterQuery := float64(jobSpecRate) / noFilterFrequency
		jobSpecQueriesToExecute = int(jobSpecQueriesPerNoFilterQuery)
		if jobSpecQueriesPerNoFilterQuery > float64(int(jobSpecQueriesPerNoFilterQuery)) {
			jobSpecQueriesToExecute++
		}
	}

	for i := 0; i < jobSpecQueriesToExecute; i++ {
		if len(jobs) == 0 {
			break
		}

		job := jobs[i%len(jobs)]
		q.queryWg.Add(1)
		go func(jobID string) {
			defer q.queryWg.Done()
			q.executeGetJobSpec(ctx, jobID)
		}(job.JobId)
	}
}

func (q *Querier) executeGetJobRunDebugMessage(ctx context.Context, runID string) {
	start := time.Now()
	_, err := q.database.GetJobRunDebugMessage(ctx, runID)
	duration := time.Since(start)

	if errors.Is(err, context.Canceled) {
		return
	}

	q.metrics.RecordQuery(metrics.QueryTypeGetJobRunDebugMessage, metrics.FilterCombinationNone, duration, err)

	if err != nil {
		logging.WithError(err).WithField("runID", runID).Warn("GetJobRunDebugMessage query failed")
	}
}

func (q *Querier) executeGetJobRunError(ctx context.Context, runID string) {
	start := time.Now()
	_, err := q.database.GetJobRunError(ctx, runID)
	duration := time.Since(start)

	if errors.Is(err, context.Canceled) {
		return
	}

	q.metrics.RecordQuery(metrics.QueryTypeGetJobRunError, metrics.FilterCombinationNone, duration, err)

	if err != nil {
		logging.WithError(err).WithField("runID", runID).Warn("GetJobRunError query failed")
	}
}

func (q *Querier) executeGetJobSpec(ctx context.Context, jobID string) {
	start := time.Now()
	_, err := q.database.GetJobSpec(ctx, jobID)
	duration := time.Since(start)

	if errors.Is(err, context.Canceled) {
		return
	}

	q.metrics.RecordQuery(metrics.QueryTypeGetJobSpec, metrics.FilterCombinationNone, duration, err)

	if err != nil {
		logging.WithError(err).WithField("jobID", jobID).Warn("GetJobSpec query failed")
	}
}
