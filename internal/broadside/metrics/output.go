//go:generate go run generate_schema.go

package metrics

import (
	"encoding/json"
	"os"
	"time"

	"github.com/armadaproject/armada/internal/broadside/configuration"
)

const SchemaVersion = "1.0.0"

type TestResult struct {
	Metadata      Metadata              `json:"metadata"`
	Configuration ConfigurationSnapshot `json:"configuration"`
	Results       Results               `json:"results"`
}

type Metadata struct {
	Timestamp    time.Time `json:"timestamp"`
	Version      string    `json:"version"`
	TestDuration string    `json:"testDuration,omitempty"`
}

type ConfigurationSnapshot struct {
	TestDuration    string                  `json:"testDuration"`
	WarmupDuration  string                  `json:"warmupDuration,omitempty"`
	DatabaseConfig  DatabaseConfigSnapshot  `json:"databaseConfig"`
	QueueConfig     []QueueConfigSnapshot   `json:"queueConfig"`
	IngestionConfig IngestionConfigSnapshot `json:"ingestionConfig"`
	QueryConfig     QueryConfigSnapshot     `json:"queryConfig"`
	ActionsConfig   *ActionsConfigSnapshot  `json:"actionsConfig,omitempty"`
}

type DatabaseConfigSnapshot struct {
	Type             string `json:"type"`
	ConnectionString string `json:"connectionString,omitempty"`
}

type QueueConfigSnapshot struct {
	Name         string                 `json:"name"`
	Proportion   float64                `json:"proportion"`
	JobSetConfig []JobSetConfigSnapshot `json:"jobSetConfig,omitempty"`
}

type JobSetConfigSnapshot struct {
	Name                 string                        `json:"name"`
	Proportion           float64                       `json:"proportion"`
	HistoricalJobsConfig *HistoricalJobsConfigSnapshot `json:"historicalJobsConfig,omitempty"`
}

type HistoricalJobsConfigSnapshot struct {
	NumberOfJobs        int     `json:"numberOfJobs"`
	ProportionSucceeded float64 `json:"proportionSucceeded"`
	ProportionErrored   float64 `json:"proportionErrored"`
	ProportionCancelled float64 `json:"proportionCancelled"`
	ProportionPreempted float64 `json:"proportionPreempted"`
}

type IngestionConfigSnapshot struct {
	BatchSize                int                               `json:"batchSize"`
	NumWorkers               int                               `json:"numWorkers"`
	SubmissionsPerHour       int                               `json:"submissionsPerHour"`
	JobStateTransitionConfig *JobStateTransitionConfigSnapshot `json:"jobStateTransitionConfig,omitempty"`
}

type JobStateTransitionConfigSnapshot struct {
	QueueingDuration         string  `json:"queueingDuration"`
	LeasedDuration           string  `json:"leasedDuration"`
	PendingDuration          string  `json:"pendingDuration"`
	ProportionSucceed        float64 `json:"proportionSucceed"`
	RunningToSuccessDuration string  `json:"runningToSuccessDuration"`
	ProportionFail           float64 `json:"proportionFail"`
	RunningToFailureDuration string  `json:"runningToFailureDuration"`
}

type QueryConfigSnapshot struct {
	GetJobRunDebugMessageQueriesPerHour int `json:"getJobRunDebugMessageQueriesPerHour"`
	GetJobRunErrorQueriesPerHour        int `json:"getJobRunErrorQueriesPerHour"`
	GetJobSpecQueriesPerHour            int `json:"getJobSpecQueriesPerHour"`
	GetJobsQueriesPerHour               int `json:"getJobsQueriesPerHour"`
	GetJobsPageSize                     int `json:"getJobsPageSize"`
	GetJobGroupsQueriesPerHour          int `json:"getJobGroupsQueriesPerHour"`
	GetJobGroupsPageSize                int `json:"getJobGroupsPageSize"`
}

type ActionsConfigSnapshot struct {
	JobSetReprioritisations []JobSetReprioritisationSnapshot `json:"jobSetReprioritisations,omitempty"`
	JobSetCancellations     []JobSetCancellationSnapshot     `json:"jobSetCancellations,omitempty"`
}

type JobSetReprioritisationSnapshot struct {
	PerformAfterTestStartTime string `json:"performAfterTestStartTime"`
	Queue                     string `json:"queue"`
	JobSet                    string `json:"jobSet"`
}

type JobSetCancellationSnapshot struct {
	PerformAfterTestStartTime string `json:"performAfterTestStartTime"`
	Queue                     string `json:"queue"`
	JobSet                    string `json:"jobSet"`
}

type Results struct {
	Ingester IngesterReportJSON `json:"ingester"`
	Querier  QuerierReportJSON  `json:"querier"`
	Actor    *ActorReportJSON   `json:"actor,omitempty"`
}

type IngesterReportJSON struct {
	TotalQueriesExecuted int     `json:"totalQueriesExecuted"`
	TotalBatchesExecuted int     `json:"totalBatchesExecuted"`
	TotalBatchesFailed   int     `json:"totalBatchesFailed"`
	PeakBacklogSize      int     `json:"peakBacklogSize"`
	AverageBacklogSize   float64 `json:"averageBacklogSize"`
	P50BacklogSize       float64 `json:"p50BacklogSize"`
	P95BacklogSize       float64 `json:"p95BacklogSize"`
	P99BacklogSize       float64 `json:"p99BacklogSize"`
	P50BacklogWaitTime   string  `json:"p50BacklogWaitTime"`
	P95BacklogWaitTime   string  `json:"p95BacklogWaitTime"`
	P99BacklogWaitTime   string  `json:"p99BacklogWaitTime"`
	MaxBacklogWaitTime   string  `json:"maxBacklogWaitTime"`
	P50ExecutionLatency  string  `json:"p50ExecutionLatency"`
	P95ExecutionLatency  string  `json:"p95ExecutionLatency"`
	P99ExecutionLatency  string  `json:"p99ExecutionLatency"`
}

type QuerierReportJSON struct {
	TotalQueriesExecuted int                    `json:"totalQueriesExecuted"`
	TotalQueriesFailed   int                    `json:"totalQueriesFailed"`
	StatsByQueryType     []QueryStatsReportJSON `json:"statsByQueryType"`
	Errors               []QueryErrorJSON       `json:"errors,omitempty"`
}

type QueryStatsReportJSON struct {
	QueryType      QueryType         `json:"queryType"`
	FilterCombo    FilterCombination `json:"filterCombo"`
	TotalExecuted  int               `json:"totalExecuted"`
	TotalFailed    int               `json:"totalFailed"`
	P50Latency     string            `json:"p50Latency"`
	P95Latency     string            `json:"p95Latency"`
	P99Latency     string            `json:"p99Latency"`
	MaxLatency     string            `json:"maxLatency"`
	AverageLatency string            `json:"averageLatency"`
}

type QueryErrorJSON struct {
	QueryType   QueryType         `json:"queryType"`
	FilterCombo FilterCombination `json:"filterCombo"`
	Error       string            `json:"error"`
	Timestamp   time.Time         `json:"timestamp"`
}

type ActorReportJSON struct {
	TotalReprioritisations     int              `json:"totalReprioritisations"`
	TotalCancellations         int              `json:"totalCancellations"`
	TotalJobsReprioritised     int              `json:"totalJobsReprioritised"`
	TotalJobsCancelled         int              `json:"totalJobsCancelled"`
	ReprioritisationP50Latency string           `json:"reprioritisationP50Latency"`
	ReprioritisationP95Latency string           `json:"reprioritisationP95Latency"`
	ReprioritisationP99Latency string           `json:"reprioritisationP99Latency"`
	CancellationP50Latency     string           `json:"cancellationP50Latency"`
	CancellationP95Latency     string           `json:"cancellationP95Latency"`
	CancellationP99Latency     string           `json:"cancellationP99Latency"`
	Errors                     []ActorErrorJSON `json:"errors,omitempty"`
}

type ActorErrorJSON struct {
	Action    string    `json:"action"`
	Queue     string    `json:"queue"`
	JobSet    string    `json:"jobSet"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
}

func ConvertConfigurationToSnapshot(config configuration.TestConfig) ConfigurationSnapshot {
	snapshot := ConfigurationSnapshot{
		TestDuration:    config.TestDuration.String(),
		WarmupDuration:  config.WarmupDuration.String(),
		DatabaseConfig:  convertDatabaseConfig(config.DatabaseConfig),
		QueueConfig:     convertQueueConfig(config.QueueConfig),
		IngestionConfig: convertIngestionConfig(config.IngestionConfig),
		QueryConfig:     convertQueryConfig(config.QueryConfig),
	}

	if hasActionsConfig(config.ActionsConfig) {
		snapshot.ActionsConfig = convertActionsConfig(config.ActionsConfig)
	}

	return snapshot
}

func convertDatabaseConfig(config configuration.DatabaseConfig) DatabaseConfigSnapshot {
	snapshot := DatabaseConfigSnapshot{}

	if len(config.Postgres) > 0 {
		snapshot.Type = "postgres"
		if connStr, ok := config.Postgres["connection"]; ok {
			snapshot.ConnectionString = connStr
		}
	} else if len(config.ClickHouse) > 0 {
		snapshot.Type = "clickhouse"
		if connStr, ok := config.ClickHouse["connection"]; ok {
			snapshot.ConnectionString = connStr
		}
	} else if config.InMemory {
		snapshot.Type = "inmemory"
	}

	return snapshot
}

func convertQueueConfig(configs []configuration.QueueConfig) []QueueConfigSnapshot {
	snapshots := make([]QueueConfigSnapshot, len(configs))
	for i, config := range configs {
		snapshots[i] = QueueConfigSnapshot{
			Name:         config.Name,
			Proportion:   config.Proportion,
			JobSetConfig: convertJobSetConfig(config.JobSetConfig),
		}
	}
	return snapshots
}

func convertJobSetConfig(configs []configuration.JobSetConfig) []JobSetConfigSnapshot {
	if len(configs) == 0 {
		return nil
	}
	snapshots := make([]JobSetConfigSnapshot, len(configs))
	for i, config := range configs {
		snapshots[i] = JobSetConfigSnapshot{
			Name:       config.Name,
			Proportion: config.Proportion,
		}
		if hasHistoricalJobsConfig(config.HistoricalJobsConfig) {
			historical := convertHistoricalJobsConfig(config.HistoricalJobsConfig)
			snapshots[i].HistoricalJobsConfig = &historical
		}
	}
	return snapshots
}

func hasHistoricalJobsConfig(config configuration.HistoricalJobsConfig) bool {
	return config.NumberOfJobs > 0
}

func convertHistoricalJobsConfig(config configuration.HistoricalJobsConfig) HistoricalJobsConfigSnapshot {
	return HistoricalJobsConfigSnapshot{
		NumberOfJobs:        config.NumberOfJobs,
		ProportionSucceeded: config.ProportionSucceeded,
		ProportionErrored:   config.ProportionErrored,
		ProportionCancelled: config.ProportionCancelled,
		ProportionPreempted: config.ProportionPreempted,
	}
}

func convertIngestionConfig(config configuration.IngestionConfig) IngestionConfigSnapshot {
	snapshot := IngestionConfigSnapshot{
		BatchSize:          config.BatchSize,
		NumWorkers:         config.NumWorkers,
		SubmissionsPerHour: config.SubmissionsPerHour,
	}

	if hasJobStateTransitionConfig(config.JobStateTransitionConfig) {
		transition := convertJobStateTransitionConfig(config.JobStateTransitionConfig)
		snapshot.JobStateTransitionConfig = &transition
	}

	return snapshot
}

func hasJobStateTransitionConfig(config configuration.JobStateTransitionConfig) bool {
	return config.QueueingDuration > 0 || config.ProportionSucceed > 0 || config.ProportionFail > 0
}

func convertJobStateTransitionConfig(config configuration.JobStateTransitionConfig) JobStateTransitionConfigSnapshot {
	return JobStateTransitionConfigSnapshot{
		QueueingDuration:         config.QueueingDuration.String(),
		LeasedDuration:           config.LeasedDuration.String(),
		PendingDuration:          config.PendingDuration.String(),
		ProportionSucceed:        config.ProportionSucceed,
		RunningToSuccessDuration: config.RunningToSuccessDuration.String(),
		ProportionFail:           config.ProportionFail,
		RunningToFailureDuration: config.RunningToFailureDuration.String(),
	}
}

func convertQueryConfig(config configuration.QueryConfig) QueryConfigSnapshot {
	return QueryConfigSnapshot{
		GetJobRunDebugMessageQueriesPerHour: config.GetJobRunDebugMessageQueriesPerHour,
		GetJobRunErrorQueriesPerHour:        config.GetJobRunErrorQueriesPerHour,
		GetJobSpecQueriesPerHour:            config.GetJobSpecQueriesPerHour,
		GetJobsQueriesPerHour:               config.GetJobsQueriesPerHour,
		GetJobsPageSize:                     config.GetJobsPageSize,
		GetJobGroupsQueriesPerHour:          config.GetJobGroupsQueriesPerHour,
		GetJobGroupsPageSize:                config.GetJobGroupsPageSize,
	}
}

func hasActionsConfig(config configuration.ActionsConfig) bool {
	return len(config.JobSetReprioritisations) > 0 || len(config.JobSetCancellations) > 0
}

func convertActionsConfig(config configuration.ActionsConfig) *ActionsConfigSnapshot {
	return &ActionsConfigSnapshot{
		JobSetReprioritisations: convertJobSetReprioritisations(config.JobSetReprioritisations),
		JobSetCancellations:     convertJobSetCancellations(config.JobSetCancellations),
	}
}

func convertJobSetReprioritisations(reprios []configuration.JobSetReprioritisation) []JobSetReprioritisationSnapshot {
	if len(reprios) == 0 {
		return nil
	}
	snapshots := make([]JobSetReprioritisationSnapshot, len(reprios))
	for i, reprio := range reprios {
		snapshots[i] = JobSetReprioritisationSnapshot{
			PerformAfterTestStartTime: reprio.PerformAfterTestStartTime.String(),
			Queue:                     reprio.Queue,
			JobSet:                    reprio.JobSet,
		}
	}
	return snapshots
}

func convertJobSetCancellations(cancellations []configuration.JobSetCancellation) []JobSetCancellationSnapshot {
	if len(cancellations) == 0 {
		return nil
	}
	snapshots := make([]JobSetCancellationSnapshot, len(cancellations))
	for i, cancel := range cancellations {
		snapshots[i] = JobSetCancellationSnapshot{
			PerformAfterTestStartTime: cancel.PerformAfterTestStartTime.String(),
			Queue:                     cancel.Queue,
			JobSet:                    cancel.JobSet,
		}
	}
	return snapshots
}

func ConvertIngesterReportToJSON(report IngesterReport) IngesterReportJSON {
	return IngesterReportJSON{
		TotalQueriesExecuted: report.TotalQueriesExecuted,
		TotalBatchesExecuted: report.TotalBatchesExecuted,
		TotalBatchesFailed:   report.TotalBatchesFailed,
		PeakBacklogSize:      report.PeakBacklogSize,
		AverageBacklogSize:   report.AverageBacklogSize,
		P50BacklogSize:       report.P50BacklogSize,
		P95BacklogSize:       report.P95BacklogSize,
		P99BacklogSize:       report.P99BacklogSize,
		P50BacklogWaitTime:   report.P50BacklogWaitTime.String(),
		P95BacklogWaitTime:   report.P95BacklogWaitTime.String(),
		P99BacklogWaitTime:   report.P99BacklogWaitTime.String(),
		MaxBacklogWaitTime:   report.MaxBacklogWaitTime.String(),
		P50ExecutionLatency:  report.P50ExecutionLatency.String(),
		P95ExecutionLatency:  report.P95ExecutionLatency.String(),
		P99ExecutionLatency:  report.P99ExecutionLatency.String(),
	}
}

func ConvertQuerierReportToJSON(report QuerierReport) QuerierReportJSON {
	json := QuerierReportJSON{
		TotalQueriesExecuted: report.TotalQueriesExecuted,
		TotalQueriesFailed:   report.TotalQueriesFailed,
		StatsByQueryType:     make([]QueryStatsReportJSON, len(report.StatsByQueryType)),
	}

	for i, stats := range report.StatsByQueryType {
		json.StatsByQueryType[i] = QueryStatsReportJSON{
			QueryType:      stats.QueryType,
			FilterCombo:    stats.FilterCombo,
			TotalExecuted:  stats.TotalExecuted,
			TotalFailed:    stats.TotalFailed,
			P50Latency:     stats.P50Latency.String(),
			P95Latency:     stats.P95Latency.String(),
			P99Latency:     stats.P99Latency.String(),
			MaxLatency:     stats.MaxLatency.String(),
			AverageLatency: stats.AverageLatency.String(),
		}
	}

	if len(report.Errors) > 0 {
		json.Errors = make([]QueryErrorJSON, len(report.Errors))
		for i, err := range report.Errors {
			json.Errors[i] = QueryErrorJSON{
				QueryType:   err.QueryType,
				FilterCombo: err.FilterCombo,
				Error:       err.Error.Error(),
				Timestamp:   err.Timestamp,
			}
		}
	}

	return json
}

func ConvertActorReportToJSON(report *ActorReport) *ActorReportJSON {
	if report == nil {
		return nil
	}

	json := &ActorReportJSON{
		TotalReprioritisations:     report.TotalReprioritisations,
		TotalCancellations:         report.TotalCancellations,
		TotalJobsReprioritised:     report.TotalJobsReprioritised,
		TotalJobsCancelled:         report.TotalJobsCancelled,
		ReprioritisationP50Latency: report.ReprioritisationP50Latency.String(),
		ReprioritisationP95Latency: report.ReprioritisationP95Latency.String(),
		ReprioritisationP99Latency: report.ReprioritisationP99Latency.String(),
		CancellationP50Latency:     report.CancellationP50Latency.String(),
		CancellationP95Latency:     report.CancellationP95Latency.String(),
		CancellationP99Latency:     report.CancellationP99Latency.String(),
	}

	if len(report.Errors) > 0 {
		json.Errors = make([]ActorErrorJSON, len(report.Errors))
		for i, err := range report.Errors {
			json.Errors[i] = ActorErrorJSON{
				Action:    err.Action,
				Queue:     err.Queue,
				JobSet:    err.JobSet,
				Error:     err.Error,
				Timestamp: err.Timestamp,
			}
		}
	}

	return json
}

func BuildTestResult(
	config configuration.TestConfig,
	ingesterReport IngesterReport,
	querierReport QuerierReport,
	actorReport *ActorReport,
	actualTestDuration time.Duration,
) TestResult {
	results := Results{
		Ingester: ConvertIngesterReportToJSON(ingesterReport),
		Querier:  ConvertQuerierReportToJSON(querierReport),
	}

	if actorReport != nil && (actorReport.TotalReprioritisations > 0 || actorReport.TotalCancellations > 0) {
		results.Actor = ConvertActorReportToJSON(actorReport)
	}

	return TestResult{
		Metadata: Metadata{
			Timestamp:    time.Now(),
			Version:      SchemaVersion,
			TestDuration: actualTestDuration.String(),
		},
		Configuration: ConvertConfigurationToSnapshot(config),
		Results:       results,
	}
}

func WriteTestResultToFile(result TestResult, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
