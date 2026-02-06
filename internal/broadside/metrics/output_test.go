package metrics

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/broadside/configuration"
)

func TestBuildTestResult(t *testing.T) {
	config := configuration.TestConfig{
		TestDuration:   30 * time.Second,
		WarmupDuration: 5 * time.Second,
		DatabaseConfig: configuration.DatabaseConfig{
			InMemory: true,
		},
		QueueConfig: []configuration.QueueConfig{
			{
				Name:       "test-queue",
				Proportion: 1.0,
			},
		},
		IngestionConfig: configuration.IngestionConfig{
			BatchSize:          10,
			NumWorkers:         2,
			SubmissionsPerHour: 1000,
		},
		QueryConfig: configuration.QueryConfig{
			GetJobsQueriesPerHour: 100,
			GetJobsPageSize:       50,
		},
	}

	ingesterReport := IngesterReport{
		TotalQueriesExecuted: 100,
		TotalBatchesExecuted: 10,
		TotalBatchesFailed:   1,
		PeakBacklogSize:      5,
		P50ExecutionLatency:  100 * time.Millisecond,
		P95ExecutionLatency:  200 * time.Millisecond,
		P99ExecutionLatency:  300 * time.Millisecond,
	}

	querierReport := QuerierReport{
		TotalQueriesExecuted: 50,
		TotalQueriesFailed:   2,
		StatsByQueryType: []QueryStatsReport{
			{
				QueryType:      QueryTypeGetJobs,
				FilterCombo:    FilterCombinationNone,
				TotalExecuted:  50,
				TotalFailed:    2,
				P50Latency:     50 * time.Millisecond,
				P95Latency:     150 * time.Millisecond,
				P99Latency:     200 * time.Millisecond,
				MaxLatency:     250 * time.Millisecond,
				AverageLatency: 75 * time.Millisecond,
			},
		},
	}

	result := BuildTestResult(config, ingesterReport, querierReport, nil, 25*time.Second)

	if result.Metadata.Version != SchemaVersion {
		t.Errorf("expected schema version %s, got %s", SchemaVersion, result.Metadata.Version)
	}

	if result.Configuration.TestDuration != "30s" {
		t.Errorf("expected test duration 30s, got %s", result.Configuration.TestDuration)
	}

	if result.Configuration.DatabaseConfig.Type != "inmemory" {
		t.Errorf("expected database type inmemory, got %s", result.Configuration.DatabaseConfig.Type)
	}

	if result.Results.Ingester.TotalQueriesExecuted != 100 {
		t.Errorf("expected 100 queries executed, got %d", result.Results.Ingester.TotalQueriesExecuted)
	}

	if result.Results.Querier.TotalQueriesExecuted != 50 {
		t.Errorf("expected 50 queries executed, got %d", result.Results.Querier.TotalQueriesExecuted)
	}

	if len(result.Results.Querier.StatsByQueryType) != 1 {
		t.Errorf("expected 1 query stat, got %d", len(result.Results.Querier.StatsByQueryType))
	}
}

func TestWriteTestResultToFile(t *testing.T) {
	config := configuration.TestConfig{
		TestDuration: 10 * time.Second,
		DatabaseConfig: configuration.DatabaseConfig{
			InMemory: true,
		},
		QueueConfig: []configuration.QueueConfig{
			{Name: "test", Proportion: 1.0},
		},
		IngestionConfig: configuration.IngestionConfig{
			BatchSize:          5,
			NumWorkers:         1,
			SubmissionsPerHour: 100,
		},
		QueryConfig: configuration.QueryConfig{
			GetJobsQueriesPerHour: 10,
		},
	}

	result := BuildTestResult(
		config,
		IngesterReport{TotalQueriesExecuted: 10},
		QuerierReport{TotalQueriesExecuted: 5},
		nil,
		9*time.Second,
	)

	tmpFile := "/tmp/test-broadside-result.json"
	defer os.Remove(tmpFile)

	err := WriteTestResultToFile(result, tmpFile)
	if err != nil {
		t.Fatalf("failed to write test result: %v", err)
	}

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("failed to read test result file: %v", err)
	}

	var readResult TestResult
	err = json.Unmarshal(data, &readResult)
	if err != nil {
		t.Fatalf("failed to unmarshal test result: %v", err)
	}

	if readResult.Configuration.TestDuration != "10s" {
		t.Errorf("expected test duration 10s after round-trip, got %s", readResult.Configuration.TestDuration)
	}

	if readResult.Results.Ingester.TotalQueriesExecuted != 10 {
		t.Errorf("expected 10 queries after round-trip, got %d", readResult.Results.Ingester.TotalQueriesExecuted)
	}
}

func TestConvertConfigurationToSnapshot(t *testing.T) {
	config := configuration.TestConfig{
		TestDuration:   1 * time.Minute,
		WarmupDuration: 10 * time.Second,
		DatabaseConfig: configuration.DatabaseConfig{
			Postgres: map[string]string{
				"connection": "postgres://localhost:5432/test",
			},
		},
		QueueConfig: []configuration.QueueConfig{
			{
				Name:       "queue1",
				Proportion: 0.6,
				JobSetConfig: []configuration.JobSetConfig{
					{
						Name:       "jobset1",
						Proportion: 0.8,
						HistoricalJobsConfig: configuration.HistoricalJobsConfig{
							NumberOfJobs:        1000,
							ProportionSucceeded: 0.9,
							ProportionErrored:   0.1,
						},
					},
				},
			},
		},
		IngestionConfig: configuration.IngestionConfig{
			BatchSize:          20,
			NumWorkers:         4,
			SubmissionsPerHour: 5000,
			JobStateTransitionConfig: configuration.JobStateTransitionConfig{
				QueueingDuration:  2 * time.Second,
				ProportionSucceed: 0.8,
			},
		},
		QueryConfig: configuration.QueryConfig{
			GetJobsQueriesPerHour: 1000,
			GetJobsPageSize:       100,
		},
	}

	snapshot := ConvertConfigurationToSnapshot(config)

	if snapshot.TestDuration != "1m0s" {
		t.Errorf("expected test duration 1m0s, got %s", snapshot.TestDuration)
	}

	if snapshot.DatabaseConfig.Type != "postgres" {
		t.Errorf("expected database type postgres, got %s", snapshot.DatabaseConfig.Type)
	}

	if snapshot.DatabaseConfig.ConnectionString != "postgres://localhost:5432/test" {
		t.Errorf("unexpected connection string: %s", snapshot.DatabaseConfig.ConnectionString)
	}

	if len(snapshot.QueueConfig) != 1 {
		t.Fatalf("expected 1 queue config, got %d", len(snapshot.QueueConfig))
	}

	if snapshot.QueueConfig[0].Name != "queue1" {
		t.Errorf("expected queue name queue1, got %s", snapshot.QueueConfig[0].Name)
	}

	if len(snapshot.QueueConfig[0].JobSetConfig) != 1 {
		t.Fatalf("expected 1 jobset config, got %d", len(snapshot.QueueConfig[0].JobSetConfig))
	}

	if snapshot.QueueConfig[0].JobSetConfig[0].HistoricalJobsConfig == nil {
		t.Error("expected historical jobs config to be present")
	} else if snapshot.QueueConfig[0].JobSetConfig[0].HistoricalJobsConfig.NumberOfJobs != 1000 {
		t.Errorf("expected 1000 historical jobs, got %d", snapshot.QueueConfig[0].JobSetConfig[0].HistoricalJobsConfig.NumberOfJobs)
	}

	if snapshot.IngestionConfig.JobStateTransitionConfig == nil {
		t.Error("expected job state transition config to be present")
	}
}
