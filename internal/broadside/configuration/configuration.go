package configuration

import "time"

type TestConfig struct {
	TestDuration    time.Duration   `yaml:"testDuration"`
	WarmupDuration  time.Duration   `yaml:"warmupDuration"`
	DatabaseConfig  DatabaseConfig  `yaml:"databaseConfig"`
	QueueConfig     []QueueConfig   `yaml:"queueConfig"`
	IngestionConfig IngestionConfig `yaml:"ingestionConfig"`
	QueryConfig     QueryConfig     `yaml:"queryConfig"`
	ActionsConfig   ActionsConfig   `yaml:"actionsConfig"`
}

type DatabaseConfig struct {
	Postgres   map[string]string `yaml:"postgres,omitempty"`
	ClickHouse map[string]string `yaml:"clickHouse,omitempty"`
	InMemory   bool              `yaml:"inMemory,omitempty"`
}

type IngestionConfig struct {
	BatchSize                   int                      `yaml:"batchSize"`
	NumWorkers                  int                      `yaml:"numWorkers"`
	SubmissionsPerHour          int                      `yaml:"submissionsPerHour"`
	ChannelBufferSizeMultiplier int                      `yaml:"channelBufferSizeMultiplier,omitempty"`
	MaxBacklogSize              int                      `yaml:"maxBacklogSize,omitempty"`
	BacklogDropStrategy         string                   `yaml:"backlogDropStrategy,omitempty"`
	JobStateTransitionConfig    JobStateTransitionConfig `yaml:"jobStateTransitionConfig"`
}

type QueueConfig struct {
	Name         string         `yaml:"name"`
	Proportion   float64        `yaml:"proportion"`
	JobSetConfig []JobSetConfig `yaml:"jobSetConfig"`
}

type JobSetConfig struct {
	Name                 string               `yaml:"name"`
	Proportion           float64              `yaml:"proportion"`
	HistoricalJobsConfig HistoricalJobsConfig `yaml:"historicalJobsConfig"`
}

type HistoricalJobsConfig struct {
	NumberOfJobs        int     `yaml:"numberOfJobs"`
	ProportionSucceeded float64 `yaml:"proportionSucceeded"`
	ProportionErrored   float64 `yaml:"proportionErrored"`
	ProportionCancelled float64 `yaml:"proportionCancelled"`
	ProportionPreempted float64 `yaml:"proportionPreempted"`
}

type JobStateTransitionConfig struct {
	QueueingDuration         time.Duration `yaml:"queueingDuration"`
	LeasedDuration           time.Duration `yaml:"leasedDuration"`
	PendingDuration          time.Duration `yaml:"pendingDuration"`
	ProportionSucceed        float64       `yaml:"proportionSucceed"`
	RunningToSuccessDuration time.Duration `yaml:"runningToSuccessDuration"`
	ProportionFail           float64       `yaml:"proportionFail"`
	RunningToFailureDuration time.Duration `yaml:"runningToFailureDuration"`
}

type ActionsConfig struct {
	JobSetReprioritisations []JobSetReprioritisation `yaml:"jobSetReprioritisations"`
	JobSetCancellations     []JobSetCancellation     `yaml:"jobSetCancellations"`
}

type JobSetReprioritisation struct {
	PerformAfterTestStartTime time.Duration `yaml:"performAfterTestStartTime"`
	Queue                     string        `yaml:"queue"`
	JobSet                    string        `yaml:"jobSet"`
}

type JobSetCancellation struct {
	PerformAfterTestStartTime time.Duration `yaml:"performAfterTestStartTime"`
	Queue                     string        `yaml:"queue"`
	JobSet                    string        `yaml:"jobSet"`
}

type QueryConfig struct {
	GetJobRunDebugMessageQueriesPerHour int `yaml:"getJobRunDebugMessageQueriesPerHour"`
	GetJobRunErrorQueriesPerHour        int `yaml:"getJobRunErrorQueriesPerHour"`
	GetJobSpecQueriesPerHour            int `yaml:"getJobSpecQueriesPerHour"`
	GetJobsQueriesPerHour               int `yaml:"getJobsQueriesPerHour"`
	GetJobsPageSize                     int `yaml:"getJobsPageSize"`
	GetJobGroupsQueriesPerHour          int `yaml:"getJobGroupsQueriesPerHour"`
	GetJobGroupsPageSize                int `yaml:"getJobGroupsPageSize"`
	MaxErrorsToCollect                  int `yaml:"maxErrorsToCollect,omitempty"`
}
