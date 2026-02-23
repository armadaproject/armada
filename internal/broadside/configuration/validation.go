package configuration

import (
	"fmt"
	"math"
)

const proportionTolerance = 0.001 // Tolerance for floating point comparison

// Validate validates the Test configuration.
func (t *TestConfig) Validate() error {
	if t.TestDuration <= 0 {
		return fmt.Errorf("testDuration must be positive, got %v", t.TestDuration)
	}
	if t.WarmupDuration < 0 {
		return fmt.Errorf("warmupDuration must be non-negative, got %v", t.WarmupDuration)
	}
	if err := t.DatabaseConfig.Validate(); err != nil {
		return fmt.Errorf("databaseConfig validation failed: %w", err)
	}
	if err := validateQueueConfigs(t.QueueConfig); err != nil {
		return err
	}
	if err := t.IngestionConfig.Validate(); err != nil {
		return fmt.Errorf("ingestionConfig validation failed: %w", err)
	}
	if err := t.QueryConfig.Validate(); err != nil {
		return fmt.Errorf("queryConfig validation failed: %w", err)
	}
	if err := t.ActionsConfig.Validate(t.TestDuration); err != nil {
		return fmt.Errorf("actionsConfig validation failed: %w", err)
	}
	return nil
}

func validateQueueConfigs(queueConfigs []QueueConfig) error {
	if len(queueConfigs) == 0 {
		return fmt.Errorf("queueConfig must contain at least one queue")
	}

	var totalProportion float64
	for idx, qc := range queueConfigs {
		if err := qc.Validate(); err != nil {
			return fmt.Errorf("queueConfig[%d] validation failed: %w", idx, err)
		}
		totalProportion += qc.Proportion
	}
	if math.Abs(totalProportion-1.0) > proportionTolerance {
		return fmt.Errorf("queueConfig proportions must sum to 1.0, got %.6f", totalProportion)
	}

	return nil
}

// Validate validates the DatabaseConfig configuration.
func (d *DatabaseConfig) Validate() error {
	postgresPopulated := len(d.Postgres) > 0
	clickHousePopulated := len(d.ClickHouse) > 0

	numPopulated := 0
	if postgresPopulated {
		numPopulated++
	}
	if clickHousePopulated {
		numPopulated++
	}
	if d.InMemory {
		numPopulated++
	}

	if numPopulated == 0 {
		return fmt.Errorf("exactly one database backend must be configured (postgres, clickHouse or in-memory)")
	}
	if numPopulated > 1 {
		return fmt.Errorf("only one database backend can be configured, but more than one of postgres, clickHouse and in-memory are populated")
	}

	// Validate the populated database has required fields
	if postgresPopulated {
		if err := validateDatabaseMap(d.Postgres, "postgres"); err != nil {
			return err
		}
	}
	if clickHousePopulated {
		if err := validateDatabaseMap(d.ClickHouse, "clickHouse"); err != nil {
			return err
		}
	}

	return nil
}

func validateDatabaseMap(dbMap map[string]string, dbType string) error {
	requiredKeys := []string{"host", "port", "database"}
	for _, key := range requiredKeys {
		if val, ok := dbMap[key]; !ok || val == "" {
			return fmt.Errorf("%s configuration must contain non-empty '%s'", dbType, key)
		}
	}
	return nil
}

// Validate validates the IngestionConfig configuration.
func (i *IngestionConfig) Validate() error {
	if i.BatchSize <= 0 {
		return fmt.Errorf("batchSize must be positive, got %d", i.BatchSize)
	}
	if i.NumWorkers < 0 {
		return fmt.Errorf("numWorkers must be non-negative, got %d", i.NumWorkers)
	}
	if i.SubmissionsPerHour < 0 {
		return fmt.Errorf("submissionsPerHour must be non-negative, got %d", i.SubmissionsPerHour)
	}
	if i.ChannelBufferSizeMultiplier < 0 {
		return fmt.Errorf("channelBufferSizeMultiplier must be non-negative, got %d", i.ChannelBufferSizeMultiplier)
	}
	if i.MaxBacklogSize < 0 {
		return fmt.Errorf("maxBacklogSize must be non-negative, got %d", i.MaxBacklogSize)
	}
	if i.BacklogDropStrategy != "" {
		validStrategies := []string{"block", "drop", "error"}
		valid := false
		for _, s := range validStrategies {
			if i.BacklogDropStrategy == s {
				valid = true
				break
			}
		}
		if !valid {
			return fmt.Errorf("backlogDropStrategy must be one of %v, got '%s'", validStrategies, i.BacklogDropStrategy)
		}
	}
	if err := i.JobStateTransitionConfig.Validate(); err != nil {
		return fmt.Errorf("jobStateTransitionConfig validation failed: %w", err)
	}
	return nil
}

// Validate validates the QueueConfig configuration.
func (q *QueueConfig) Validate() error {
	if q.Name == "" {
		return fmt.Errorf("queue name must not be empty")
	}
	if q.Proportion < 0 || q.Proportion > 1 {
		return fmt.Errorf("queue '%s' proportion must be in range [0, 1], got %.6f", q.Name, q.Proportion)
	}
	if len(q.JobSetConfig) == 0 {
		return fmt.Errorf("queue '%s' must contain at least one jobSetConfig", q.Name)
	}

	// Validate jobset proportions sum to 1.0
	var totalProportion float64
	for idx, jsc := range q.JobSetConfig {
		if err := jsc.Validate(); err != nil {
			return fmt.Errorf("queue '%s' jobSetConfig[%d] validation failed: %w", q.Name, idx, err)
		}
		totalProportion += jsc.Proportion
	}
	if math.Abs(totalProportion-1.0) > proportionTolerance {
		return fmt.Errorf("queue '%s' jobSetConfig proportions must sum to 1.0, got %.6f", q.Name, totalProportion)
	}

	return nil
}

// Validate validates the JobSetConfig configuration.
func (j *JobSetConfig) Validate() error {
	if j.Name == "" {
		return fmt.Errorf("jobSet name must not be empty")
	}
	if j.Proportion < 0 || j.Proportion > 1 {
		return fmt.Errorf("jobSet '%s' proportion must be in range [0, 1], got %.6f", j.Name, j.Proportion)
	}
	if err := j.HistoricalJobsConfig.Validate(); err != nil {
		return fmt.Errorf("jobSet '%s' historicalJobsConfig validation failed: %w", j.Name, err)
	}
	return nil
}

// Validate validates the HistoricalJobsConfig configuration.
func (h *HistoricalJobsConfig) Validate() error {
	if h.NumberOfJobs < 0 {
		return fmt.Errorf("numberOfJobs must be non-negative, got %d", h.NumberOfJobs)
	}

	proportions := []struct {
		name  string
		value float64
	}{
		{"proportionSucceeded", h.ProportionSucceeded},
		{"proportionErrored", h.ProportionErrored},
		{"proportionCancelled", h.ProportionCancelled},
		{"proportionPreempted", h.ProportionPreempted},
	}

	for _, p := range proportions {
		if p.value < 0 || p.value > 1 {
			return fmt.Errorf("%s must be in range [0, 1], got %.6f", p.name, p.value)
		}
	}

	totalProportion := h.ProportionSucceeded + h.ProportionErrored + h.ProportionCancelled + h.ProportionPreempted
	if totalProportion > 1.0+proportionTolerance {
		return fmt.Errorf("sum of all proportions must not exceed 1.0, got %.6f", totalProportion)
	}

	return nil
}

// Validate validates the JobStateTransitionConfig configuration.
func (j *JobStateTransitionConfig) Validate() error {
	durations := []struct {
		name  string
		value interface{}
	}{
		{"queueingDuration", j.QueueingDuration},
		{"pendingDuration", j.PendingDuration},
		{"leasedDuration", j.LeasedDuration},
		{"runningToSuccessDuration", j.RunningToSuccessDuration},
		{"runningToFailureDuration", j.RunningToFailureDuration},
	}

	for _, d := range durations {
		if d.value.(interface{ Nanoseconds() int64 }).Nanoseconds() < 0 {
			return fmt.Errorf("%s must be non-negative, got %v", d.name, d.value)
		}
	}

	if j.ProportionSucceed < 0 || j.ProportionSucceed > 1 {
		return fmt.Errorf("proportionSucceed must be in range [0, 1], got %.6f", j.ProportionSucceed)
	}
	if j.ProportionFail < 0 || j.ProportionFail > 1 {
		return fmt.Errorf("proportionFail must be in range [0, 1], got %.6f", j.ProportionFail)
	}

	totalProportion := j.ProportionSucceed + j.ProportionFail
	if totalProportion > 1.0+proportionTolerance {
		return fmt.Errorf("proportionSucceed + proportionFail must not exceed 1.0, got %.6f", totalProportion)
	}

	return nil
}

// Validate validates the ActionsConfig configuration.
func (a *ActionsConfig) Validate(testDuration interface{}) error {
	testDurationNano := testDuration.(interface{ Nanoseconds() int64 }).Nanoseconds()

	for idx, jsr := range a.JobSetReprioritisations {
		if err := jsr.Validate(testDurationNano); err != nil {
			return fmt.Errorf("jobSetReprioritisations[%d] validation failed: %w", idx, err)
		}
	}

	for idx, jsc := range a.JobSetCancellations {
		if err := jsc.Validate(testDurationNano); err != nil {
			return fmt.Errorf("jobSetCancellations[%d] validation failed: %w", idx, err)
		}
	}

	return nil
}

// Validate validates the JobSetReprioritisation configuration.
func (j *JobSetReprioritisation) Validate(testDurationNano int64) error {
	if j.PerformAfterTestStartTime.Nanoseconds() < 0 {
		return fmt.Errorf("performAfterTestStartTime must be non-negative, got %v", j.PerformAfterTestStartTime)
	}
	if j.PerformAfterTestStartTime.Nanoseconds() > testDurationNano {
		return fmt.Errorf("performAfterTestStartTime (%v) must not exceed test duration", j.PerformAfterTestStartTime)
	}
	if j.Queue == "" {
		return fmt.Errorf("queue must not be empty")
	}
	if j.JobSet == "" {
		return fmt.Errorf("jobSet must not be empty")
	}
	return nil
}

// Validate validates the JobSetCancellation configuration.
func (j *JobSetCancellation) Validate(testDurationNano int64) error {
	if j.PerformAfterTestStartTime.Nanoseconds() < 0 {
		return fmt.Errorf("performAfterTestStartTime must be non-negative, got %v", j.PerformAfterTestStartTime)
	}
	if j.PerformAfterTestStartTime.Nanoseconds() > testDurationNano {
		return fmt.Errorf("performAfterTestStartTime (%v) must not exceed test duration", j.PerformAfterTestStartTime)
	}
	if j.Queue == "" {
		return fmt.Errorf("queue must not be empty")
	}
	if j.JobSet == "" {
		return fmt.Errorf("jobSet must not be empty")
	}
	return nil
}

// Validate validates the QueryConfig configuration.
func (q *QueryConfig) Validate() error {
	queries := []struct {
		name  string
		value int
	}{
		{"getJobRunDebugMessageQueriesPerHour", q.GetJobRunDebugMessageQueriesPerHour},
		{"getJobRunErrorQueriesPerHour", q.GetJobRunErrorQueriesPerHour},
		{"getJobsQueriesPerHour", q.GetJobsQueriesPerHour},
		{"getJobGroupsQueriesPerHour", q.GetJobGroupsQueriesPerHour},
		{"getJobSpecQueriesPerHour", q.GetJobSpecQueriesPerHour},
	}

	for _, qry := range queries {
		if qry.value < 0 {
			return fmt.Errorf("%s must be non-negative, got %d", qry.name, qry.value)
		}
	}

	if q.MaxErrorsToCollect < 0 {
		return fmt.Errorf("maxErrorsToCollect must be non-negative, got %d", q.MaxErrorsToCollect)
	}

	return nil
}
