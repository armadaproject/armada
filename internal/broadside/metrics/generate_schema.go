//go:build ignore

package main

import (
	"encoding/json"
	"os"

	"github.com/invopop/jsonschema"
)

// This mirrors the TestResult structure for schema generation
type TestResult struct {
	Metadata      Metadata              `json:"metadata" jsonschema:"required"`
	Configuration ConfigurationSnapshot `json:"configuration" jsonschema:"required"`
	Results       Results               `json:"results" jsonschema:"required"`
}

type Metadata struct {
	Timestamp    string `json:"timestamp" jsonschema:"required,format=date-time" jsonschema_description:"ISO 8601 timestamp when the test was completed"`
	Version      string `json:"version" jsonschema:"required" jsonschema_description:"Schema version for compatibility tracking"`
	TestDuration string `json:"testDuration,omitempty" jsonschema_description:"Actual duration of the test (ISO 8601 duration format)"`
}

type ConfigurationSnapshot struct {
	TestDuration    string                  `json:"testDuration" jsonschema:"required" jsonschema_description:"Configured test duration (ISO 8601 duration format)"`
	WarmupDuration  string                  `json:"warmupDuration,omitempty" jsonschema_description:"Warmup duration before metrics collection begins (ISO 8601 duration format)"`
	DatabaseConfig  DatabaseConfigSnapshot  `json:"databaseConfig" jsonschema:"required"`
	QueueConfig     []QueueConfigSnapshot   `json:"queueConfig" jsonschema:"required"`
	IngestionConfig IngestionConfigSnapshot `json:"ingestionConfig" jsonschema:"required"`
	QueryConfig     QueryConfigSnapshot     `json:"queryConfig" jsonschema:"required"`
	ActionsConfig   *ActionsConfigSnapshot  `json:"actionsConfig,omitempty"`
}

type DatabaseConfigSnapshot struct {
	Type             string `json:"type" jsonschema:"enum=postgres,enum=clickhouse,enum=inmemory" jsonschema_description:"Database backend type used for the test"`
	ConnectionString string `json:"connectionString,omitempty" jsonschema_description:"Database connection details (sensitive data should be redacted)"`
}

type QueueConfigSnapshot struct {
	Name         string                 `json:"name" jsonschema:"required"`
	Proportion   float64                `json:"proportion" jsonschema:"required"`
	JobSetConfig []JobSetConfigSnapshot `json:"jobSetConfig,omitempty"`
}

type JobSetConfigSnapshot struct {
	Name                 string                        `json:"name" jsonschema:"required"`
	Proportion           float64                       `json:"proportion" jsonschema:"required"`
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
	BatchSize                int                               `json:"batchSize" jsonschema:"required"`
	NumWorkers               int                               `json:"numWorkers" jsonschema:"required"`
	SubmissionsPerHour       int                               `json:"submissionsPerHour" jsonschema:"required"`
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
	Ingester IngesterReportJSON `json:"ingester" jsonschema:"required"`
	Querier  QuerierReportJSON  `json:"querier" jsonschema:"required"`
}

type IngesterReportJSON struct {
	TotalQueriesExecuted int     `json:"totalQueriesExecuted" jsonschema:"required" jsonschema_description:"Total number of database queries executed by the ingester"`
	TotalBatchesExecuted int     `json:"totalBatchesExecuted" jsonschema:"required" jsonschema_description:"Total number of batches processed"`
	TotalBatchesFailed   int     `json:"totalBatchesFailed" jsonschema:"required" jsonschema_description:"Number of batches that failed"`
	PeakBacklogSize      int     `json:"peakBacklogSize" jsonschema:"required" jsonschema_description:"Maximum backlog size observed during the test"`
	AverageBacklogSize   float64 `json:"averageBacklogSize" jsonschema_description:"Average backlog size"`
	P50BacklogSize       float64 `json:"p50BacklogSize" jsonschema_description:"50th percentile backlog size"`
	P95BacklogSize       float64 `json:"p95BacklogSize" jsonschema_description:"95th percentile backlog size"`
	P99BacklogSize       float64 `json:"p99BacklogSize" jsonschema_description:"99th percentile backlog size"`
	P50BacklogWaitTime   string  `json:"p50BacklogWaitTime" jsonschema_description:"50th percentile wait time in backlog"`
	P95BacklogWaitTime   string  `json:"p95BacklogWaitTime" jsonschema_description:"95th percentile wait time in backlog"`
	P99BacklogWaitTime   string  `json:"p99BacklogWaitTime" jsonschema_description:"99th percentile wait time in backlog"`
	MaxBacklogWaitTime   string  `json:"maxBacklogWaitTime" jsonschema_description:"Maximum wait time in backlog"`
	P50ExecutionLatency  string  `json:"p50ExecutionLatency" jsonschema_description:"50th percentile batch execution latency"`
	P95ExecutionLatency  string  `json:"p95ExecutionLatency" jsonschema_description:"95th percentile batch execution latency"`
	P99ExecutionLatency  string  `json:"p99ExecutionLatency" jsonschema_description:"99th percentile batch execution latency"`
}

type QuerierReportJSON struct {
	TotalQueriesExecuted int                    `json:"totalQueriesExecuted" jsonschema:"required" jsonschema_description:"Total number of queries executed"`
	TotalQueriesFailed   int                    `json:"totalQueriesFailed" jsonschema:"required" jsonschema_description:"Total number of queries that failed"`
	StatsByQueryType     []QueryStatsReportJSON `json:"statsByQueryType" jsonschema:"required" jsonschema_description:"Statistics broken down by query type and filter combination"`
	Errors               []QueryErrorJSON       `json:"errors,omitempty" jsonschema_description:"List of errors that occurred during queries"`
}

type QueryStatsReportJSON struct {
	QueryType      string `json:"queryType" jsonschema:"required" jsonschema_description:"Type of query (e.g., GetJobs, GetJobGroups)"`
	FilterCombo    string `json:"filterCombo" jsonschema:"required" jsonschema_description:"Filter combination used"`
	TotalExecuted  int    `json:"totalExecuted" jsonschema:"required"`
	TotalFailed    int    `json:"totalFailed" jsonschema:"required"`
	P50Latency     string `json:"p50Latency" jsonschema_description:"50th percentile latency"`
	P95Latency     string `json:"p95Latency" jsonschema_description:"95th percentile latency"`
	P99Latency     string `json:"p99Latency" jsonschema_description:"99th percentile latency"`
	MaxLatency     string `json:"maxLatency" jsonschema_description:"Maximum latency observed"`
	AverageLatency string `json:"averageLatency" jsonschema_description:"Average latency"`
}

type QueryErrorJSON struct {
	QueryType   string `json:"queryType"`
	FilterCombo string `json:"filterCombo"`
	Error       string `json:"error"`
	Timestamp   string `json:"timestamp" jsonschema:"format=date-time"`
}

func main() {
	reflector := jsonschema.Reflector{
		AllowAdditionalProperties: false,
		DoNotReference:            true,
	}

	schema := reflector.Reflect(&TestResult{})
	schema.ID = jsonschema.ID("https://armadaproject.io/broadside/test-result.schema.json")
	schema.Title = "Broadside Load Test Result"
	schema.Description = "Complete output of a Broadside load test including configuration and metrics"
	schema.Version = "1.0.0"

	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic(err)
	}

	if err := os.WriteFile("schema.json", data, 0644); err != nil {
		panic(err)
	}
}
