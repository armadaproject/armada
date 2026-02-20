package configuration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTest_Validate(t *testing.T) {
	validTest := &TestConfig{
		TestDuration:   1 * time.Hour,
		WarmupDuration: 5 * time.Minute,
		DatabaseConfig: DatabaseConfig{
			Postgres: map[string]string{
				"host":     "localhost",
				"port":     "5432",
				"database": "testdb",
			},
		},
		QueueConfig: []QueueConfig{
			{
				Name:       "queue1",
				Proportion: 1.0,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           1.0,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
		},
		IngestionConfig: IngestionConfig{
			BatchSize:          100,
			SubmissionsPerHour: 5000,
			JobStateTransitionConfig: JobStateTransitionConfig{
				ProportionSucceed: 0.9,
				ProportionFail:    0.1,
			},
		},
		QueryConfig:   QueryConfig{},
		ActionsConfig: ActionsConfig{},
	}

	tests := []struct {
		name    string
		modify  func(*TestConfig)
		wantErr bool
		errText string
	}{
		{
			name:    "valid configuration",
			modify:  func(t *TestConfig) {},
			wantErr: false,
		},
		{
			name: "zero test duration",
			modify: func(t *TestConfig) {
				t.TestDuration = 0
			},
			wantErr: true,
			errText: "testDuration must be positive",
		},
		{
			name: "negative test duration",
			modify: func(t *TestConfig) {
				t.TestDuration = -1 * time.Hour
			},
			wantErr: true,
			errText: "testDuration must be positive",
		},
		{
			name: "negative warmup duration",
			modify: func(t *TestConfig) {
				t.WarmupDuration = -1 * time.Minute
			},
			wantErr: true,
			errText: "warmupDuration must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCopy := *validTest
			tt.modify(&testCopy)
			err := testCopy.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDatabaseConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  DatabaseConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid postgres config",
			config: DatabaseConfig{
				Postgres: map[string]string{
					"host":     "localhost",
					"port":     "5432",
					"database": "testdb",
				},
			},
			wantErr: false,
		},
		{
			name: "valid clickhouse config",
			config: DatabaseConfig{
				ClickHouse: map[string]string{
					"host":     "localhost",
					"port":     "9000",
					"database": "testdb",
				},
			},
			wantErr: false,
		},
		{
			name: "valid in-memory database config",
			config: DatabaseConfig{
				InMemory: true,
			},
			wantErr: false,
		},
		{
			name:    "no database configured",
			config:  DatabaseConfig{},
			wantErr: true,
			errText: "exactly one database backend must be configured",
		},
		{
			name: "postgres and clickhouse databases configured",
			config: DatabaseConfig{
				Postgres: map[string]string{
					"host":     "localhost",
					"port":     "5432",
					"database": "testdb",
				},
				ClickHouse: map[string]string{
					"host":     "localhost",
					"port":     "9000",
					"database": "testdb",
				},
			},
			wantErr: true,
			errText: "only one database backend can be configured",
		},
		{
			name: "clickhouse and in-memory databases configured",
			config: DatabaseConfig{
				ClickHouse: map[string]string{
					"host":     "localhost",
					"port":     "9000",
					"database": "testdb",
				},
				InMemory: true,
			},
			wantErr: true,
			errText: "only one database backend can be configured",
		},
		{
			name: "postgres missing host",
			config: DatabaseConfig{
				Postgres: map[string]string{
					"port":     "5432",
					"database": "testdb",
				},
			},
			wantErr: true,
			errText: "must contain non-empty 'host'",
		},
		{
			name: "postgres missing port",
			config: DatabaseConfig{
				Postgres: map[string]string{
					"host":     "localhost",
					"database": "testdb",
				},
			},
			wantErr: true,
			errText: "must contain non-empty 'port'",
		},
		{
			name: "postgres missing database",
			config: DatabaseConfig{
				Postgres: map[string]string{
					"host": "localhost",
					"port": "5432",
				},
			},
			wantErr: true,
			errText: "must contain non-empty 'database'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIngestionConfig_Validate(t *testing.T) {
	validConfig := IngestionConfig{
		BatchSize:          100,
		SubmissionsPerHour: 5000,
		JobStateTransitionConfig: JobStateTransitionConfig{
			ProportionSucceed: 0.8,
			ProportionFail:    0.2,
		},
	}

	tests := []struct {
		name    string
		modify  func(*IngestionConfig)
		wantErr bool
		errText string
	}{
		{
			name:    "valid config",
			modify:  func(c *IngestionConfig) {},
			wantErr: false,
		},
		{
			name: "zero batch size",
			modify: func(c *IngestionConfig) {
				c.BatchSize = 0
			},
			wantErr: true,
			errText: "batchSize must be positive",
		},
		{
			name: "negative submissions per hour",
			modify: func(c *IngestionConfig) {
				c.SubmissionsPerHour = -1
			},
			wantErr: true,
			errText: "submissionsPerHour must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configCopy := validConfig
			tt.modify(&configCopy)
			err := configCopy.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateQueueConfigs(t *testing.T) {
	tests := []struct {
		name    string
		configs []QueueConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config with single queue",
			configs: []QueueConfig{
				{
					Name:       "queue1",
					Proportion: 1.0,
					JobSetConfig: []JobSetConfig{
						{
							Name:                 "jobset1",
							Proportion:           1.0,
							HistoricalJobsConfig: HistoricalJobsConfig{},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with multiple queues",
			configs: []QueueConfig{
				{
					Name:       "queue1",
					Proportion: 0.6,
					JobSetConfig: []JobSetConfig{
						{
							Name:                 "jobset1",
							Proportion:           1.0,
							HistoricalJobsConfig: HistoricalJobsConfig{},
						},
					},
				},
				{
					Name:       "queue2",
					Proportion: 0.4,
					JobSetConfig: []JobSetConfig{
						{
							Name:                 "jobset1",
							Proportion:           1.0,
							HistoricalJobsConfig: HistoricalJobsConfig{},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "no queues",
			configs: []QueueConfig{},
			wantErr: true,
			errText: "queueConfig must contain at least one queue",
		},
		{
			name: "queue proportions don't sum to 1",
			configs: []QueueConfig{
				{
					Name:       "queue1",
					Proportion: 0.5,
					JobSetConfig: []JobSetConfig{
						{
							Name:                 "jobset1",
							Proportion:           1.0,
							HistoricalJobsConfig: HistoricalJobsConfig{},
						},
					},
				},
				{
					Name:       "queue2",
					Proportion: 0.4,
					JobSetConfig: []JobSetConfig{
						{
							Name:                 "jobset1",
							Proportion:           1.0,
							HistoricalJobsConfig: HistoricalJobsConfig{},
						},
					},
				},
			},
			wantErr: true,
			errText: "queueConfig proportions must sum to 1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateQueueConfigs(tt.configs)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueueConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  QueueConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: QueueConfig{
				Name:       "queue1",
				Proportion: 0.7,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           1.0,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty name",
			config: QueueConfig{
				Name:       "",
				Proportion: 0.7,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           1.0,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
			wantErr: true,
			errText: "queue name must not be empty",
		},
		{
			name: "zero proportion",
			config: QueueConfig{
				Name:       "queue1",
				Proportion: 0,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           1.0,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "negative proportion",
			config: QueueConfig{
				Name:       "queue1",
				Proportion: -0.1,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           1.0,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
			wantErr: true,
			errText: "proportion must be in range [0, 1]",
		},
		{
			name: "proportion greater than 1",
			config: QueueConfig{
				Name:       "queue1",
				Proportion: 1.5,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           1.0,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
			wantErr: true,
			errText: "proportion must be in range [0, 1]",
		},
		{
			name: "no jobsets",
			config: QueueConfig{
				Name:         "queue1",
				Proportion:   0.7,
				JobSetConfig: []JobSetConfig{},
			},
			wantErr: true,
			errText: "must contain at least one jobSetConfig",
		},
		{
			name: "jobset proportions don't sum to 1",
			config: QueueConfig{
				Name:       "queue1",
				Proportion: 0.7,
				JobSetConfig: []JobSetConfig{
					{
						Name:                 "jobset1",
						Proportion:           0.5,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
					{
						Name:                 "jobset2",
						Proportion:           0.4,
						HistoricalJobsConfig: HistoricalJobsConfig{},
					},
				},
			},
			wantErr: true,
			errText: "jobSetConfig proportions must sum to 1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJobSetConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  JobSetConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: JobSetConfig{
				Name:                 "jobset1",
				Proportion:           0.8,
				HistoricalJobsConfig: HistoricalJobsConfig{},
			},
			wantErr: false,
		},
		{
			name: "empty name",
			config: JobSetConfig{
				Name:                 "",
				Proportion:           0.8,
				HistoricalJobsConfig: HistoricalJobsConfig{},
			},
			wantErr: true,
			errText: "jobSet name must not be empty",
		},
		{
			name: "zero proportion",
			config: JobSetConfig{
				Name:                 "jobset1",
				Proportion:           0,
				HistoricalJobsConfig: HistoricalJobsConfig{},
			},
			wantErr: false,
		},
		{
			name: "negative proportion",
			config: JobSetConfig{
				Name:                 "jobset1",
				Proportion:           -0.1,
				HistoricalJobsConfig: HistoricalJobsConfig{},
			},
			wantErr: true,
			errText: "proportion must be in range [0, 1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHistoricalJobsConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  HistoricalJobsConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config with all proportions",
			config: HistoricalJobsConfig{
				NumberOfJobs:        1000,
				ProportionSucceeded: 0.7,
				ProportionErrored:   0.1,
				ProportionCancelled: 0.1,
				ProportionPreempted: 0.1,
			},
			wantErr: false,
		},
		{
			name: "valid config with partial proportions",
			config: HistoricalJobsConfig{
				NumberOfJobs:        1000,
				ProportionSucceeded: 0.5,
				ProportionErrored:   0.3,
			},
			wantErr: false,
		},
		{
			name: "negative number of jobs",
			config: HistoricalJobsConfig{
				NumberOfJobs: -1,
			},
			wantErr: true,
			errText: "numberOfJobs must be non-negative",
		},
		{
			name: "proportion succeeded greater than 1",
			config: HistoricalJobsConfig{
				NumberOfJobs:        1000,
				ProportionSucceeded: 1.5,
			},
			wantErr: true,
			errText: "proportionSucceeded must be in range [0, 1]",
		},
		{
			name: "negative proportion errored",
			config: HistoricalJobsConfig{
				NumberOfJobs:      1000,
				ProportionErrored: -0.1,
			},
			wantErr: true,
			errText: "proportionErrored must be in range [0, 1]",
		},
		{
			name: "sum of proportions exceeds 1",
			config: HistoricalJobsConfig{
				NumberOfJobs:        1000,
				ProportionSucceeded: 0.6,
				ProportionErrored:   0.3,
				ProportionCancelled: 0.3,
			},
			wantErr: true,
			errText: "sum of all proportions must not exceed 1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJobStateTransitionConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  JobStateTransitionConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: JobStateTransitionConfig{
				QueueingDuration:         30 * time.Second,
				PendingDuration:          10 * time.Second,
				LeasedDuration:           5 * time.Second,
				ProportionSucceed:        0.9,
				RunningToSuccessDuration: 5 * time.Minute,
				ProportionFail:           0.1,
				RunningToFailureDuration: 2 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "negative queueing duration",
			config: JobStateTransitionConfig{
				QueueingDuration:  -1 * time.Second,
				ProportionSucceed: 1.0,
			},
			wantErr: true,
			errText: "queueingDuration must be non-negative",
		},
		{
			name: "proportion succeed greater than 1",
			config: JobStateTransitionConfig{
				ProportionSucceed: 1.5,
			},
			wantErr: true,
			errText: "proportionSucceed must be in range [0, 1]",
		},
		{
			name: "negative proportion fail",
			config: JobStateTransitionConfig{
				ProportionSucceed: 0.8,
				ProportionFail:    -0.1,
			},
			wantErr: true,
			errText: "proportionFail must be in range [0, 1]",
		},
		{
			name: "sum of proportions exceeds 1",
			config: JobStateTransitionConfig{
				ProportionSucceed: 0.7,
				ProportionFail:    0.5,
			},
			wantErr: true,
			errText: "proportionSucceed + proportionFail must not exceed 1.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestActionsConfig_Validate(t *testing.T) {
	testDuration := 1 * time.Hour

	tests := []struct {
		name    string
		config  ActionsConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: ActionsConfig{
				JobSetReprioritisations: []JobSetReprioritisation{
					{
						PerformAfterTestStartTime: 30 * time.Minute,
						Queue:                     "queue1",
						JobSet:                    "jobset1",
					},
				},
				JobSetCancellations: []JobSetCancellation{
					{
						PerformAfterTestStartTime: 45 * time.Minute,
						Queue:                     "queue1",
						JobSet:                    "jobset2",
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "empty config",
			config:  ActionsConfig{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate(&testDuration)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJobSetReprioritisation_Validate(t *testing.T) {
	testDurationNano := (1 * time.Hour).Nanoseconds()

	tests := []struct {
		name    string
		config  JobSetReprioritisation
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: JobSetReprioritisation{
				PerformAfterTestStartTime: 30 * time.Minute,
				Queue:                     "queue1",
				JobSet:                    "jobset1",
			},
			wantErr: false,
		},
		{
			name: "negative perform after time",
			config: JobSetReprioritisation{
				PerformAfterTestStartTime: -1 * time.Minute,
				Queue:                     "queue1",
				JobSet:                    "jobset1",
			},
			wantErr: true,
			errText: "performAfterTestStartTime must be non-negative",
		},
		{
			name: "perform after time exceeds test duration",
			config: JobSetReprioritisation{
				PerformAfterTestStartTime: 2 * time.Hour,
				Queue:                     "queue1",
				JobSet:                    "jobset1",
			},
			wantErr: true,
			errText: "must not exceed test duration",
		},
		{
			name: "empty queue",
			config: JobSetReprioritisation{
				PerformAfterTestStartTime: 30 * time.Minute,
				Queue:                     "",
				JobSet:                    "jobset1",
			},
			wantErr: true,
			errText: "queue must not be empty",
		},
		{
			name: "empty jobset",
			config: JobSetReprioritisation{
				PerformAfterTestStartTime: 30 * time.Minute,
				Queue:                     "queue1",
				JobSet:                    "",
			},
			wantErr: true,
			errText: "jobSet must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate(testDurationNano)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJobSetCancellation_Validate(t *testing.T) {
	testDurationNano := (1 * time.Hour).Nanoseconds()

	tests := []struct {
		name    string
		config  JobSetCancellation
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: JobSetCancellation{
				PerformAfterTestStartTime: 30 * time.Minute,
				Queue:                     "queue1",
				JobSet:                    "jobset1",
			},
			wantErr: false,
		},
		{
			name: "negative perform after time",
			config: JobSetCancellation{
				PerformAfterTestStartTime: -1 * time.Minute,
				Queue:                     "queue1",
				JobSet:                    "jobset1",
			},
			wantErr: true,
			errText: "performAfterTestStartTime must be non-negative",
		},
		{
			name: "perform after time exceeds test duration",
			config: JobSetCancellation{
				PerformAfterTestStartTime: 2 * time.Hour,
				Queue:                     "queue1",
				JobSet:                    "jobset1",
			},
			wantErr: true,
			errText: "must not exceed test duration",
		},
		{
			name: "empty queue",
			config: JobSetCancellation{
				PerformAfterTestStartTime: 30 * time.Minute,
				Queue:                     "",
				JobSet:                    "jobset1",
			},
			wantErr: true,
			errText: "queue must not be empty",
		},
		{
			name: "empty jobset",
			config: JobSetCancellation{
				PerformAfterTestStartTime: 30 * time.Minute,
				Queue:                     "queue1",
				JobSet:                    "",
			},
			wantErr: true,
			errText: "jobSet must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate(testDurationNano)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestQueryConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  QueryConfig
		wantErr bool
		errText string
	}{
		{
			name: "valid config",
			config: QueryConfig{
				GetJobRunDebugMessageQueriesPerHour: 100,
				GetJobRunErrorQueriesPerHour:        50,
				GetJobsQueriesPerHour:               200,
				GetJobGroupsQueriesPerHour:          150,
				GetJobSpecQueriesPerHour:            75,
			},
			wantErr: false,
		},
		{
			name: "all zeros",
			config: QueryConfig{
				GetJobRunDebugMessageQueriesPerHour: 0,
				GetJobRunErrorQueriesPerHour:        0,
				GetJobsQueriesPerHour:               0,
				GetJobGroupsQueriesPerHour:          0,
				GetJobSpecQueriesPerHour:            0,
			},
			wantErr: false,
		},
		{
			name: "negative value",
			config: QueryConfig{
				GetJobRunDebugMessageQueriesPerHour: -1,
			},
			wantErr: true,
			errText: "getJobRunDebugMessageQueriesPerHour must be non-negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errText)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
