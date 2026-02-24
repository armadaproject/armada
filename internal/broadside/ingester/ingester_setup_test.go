package ingester_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/db"
	"github.com/armadaproject/armada/internal/broadside/ingester"
	"github.com/armadaproject/armada/internal/broadside/metrics"
)

func TestIngester_Setup_PopulatesExpectedJobCount(t *testing.T) {
	mem := db.NewMemoryDatabase()
	require.NoError(t, mem.InitialiseSchema(context.Background()))

	queueCfgs := []configuration.QueueConfig{
		{
			Name:       "q1",
			Proportion: 0.6,
			JobSetConfig: []configuration.JobSetConfig{
				{
					Name:       "js1",
					Proportion: 0.5,
					HistoricalJobsConfig: configuration.HistoricalJobsConfig{
						NumberOfJobs:        50,
						ProportionSucceeded: 0.8,
						ProportionErrored:   0.1,
						ProportionCancelled: 0.05,
						ProportionPreempted: 0.05,
					},
				},
				{
					Name:       "js2",
					Proportion: 0.5,
					HistoricalJobsConfig: configuration.HistoricalJobsConfig{
						NumberOfJobs:        30,
						ProportionSucceeded: 1.0,
					},
				},
			},
		},
		{
			Name:       "q2",
			Proportion: 0.4,
			JobSetConfig: []configuration.JobSetConfig{
				{
					Name:       "js3",
					Proportion: 1.0,
					HistoricalJobsConfig: configuration.HistoricalJobsConfig{
						NumberOfJobs:        20,
						ProportionSucceeded: 0.5,
						ProportionErrored:   0.5,
					},
				},
			},
		},
	}

	cfg := configuration.IngestionConfig{
		BatchSize:  100,
		NumWorkers: 1,
	}

	ing, err := ingester.NewIngester(cfg, queueCfgs, mem, metrics.NewIngesterMetrics())
	require.NoError(t, err)

	err = ing.Setup(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	jobs, err := mem.GetJobs(&ctx, nil, false, nil, 0, 1000)
	require.NoError(t, err)
	// Total: 50 + 30 + 20 = 100
	assert.Len(t, jobs, 100)
}
