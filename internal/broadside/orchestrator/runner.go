package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/common/logging"

	"github.com/armadaproject/armada/internal/broadside/actions"
	"github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/db"
	"github.com/armadaproject/armada/internal/broadside/ingester"
	"github.com/armadaproject/armada/internal/broadside/metrics"
	"github.com/armadaproject/armada/internal/broadside/querier"
)

const defaultResultsDir = "cmd/broadside/results"

// Runner orchestrates a Broadside load test. It initialises the database,
// starts the ingester, manages the test lifecycle, and collects metrics.
type Runner struct {
	config     configuration.TestConfig
	resultsDir string
}

// NewRunner creates a new Runner with the given test configuration.
// If resultsDir is empty, results are written to the default directory
// (cmd/broadside/results).
func NewRunner(config configuration.TestConfig, resultsDir string) *Runner {
	if resultsDir == "" {
		resultsDir = defaultResultsDir
	}
	return &Runner{
		config:     config,
		resultsDir: resultsDir,
	}
}

// Run executes the load test, orchestrating the complete test lifecycle.
//
// It performs the following steps:
//  1. Creates and initialises the database backend
//  2. Starts the ingester and querier in separate goroutines
//  3. If configured, waits for a warmup period before resetting metrics
//  4. Waits for the test duration to complete
//  5. Tears down the database
//  6. Writes metrics reports and configuration to a timestamped JSON file in the results directory
//
// The warmup period allows the system to reach steady state before
// measurement begins. During warmup, both ingester and querier metrics
// are reset to exclude initial ramp-up behaviour from the final report.
//
// If the context is cancelled during execution, the function returns
// immediately after waiting for running goroutines to complete.
func (r *Runner) Run(ctx context.Context) error {
	logging.Info("Starting Broadside load test")
	logging.Infof("Test duration: %v, Warmup duration: %v", r.config.TestDuration, r.config.WarmupDuration)

	logging.Info("Initialising database connection")
	database, err := r.newDatabase()
	if err != nil {
		return fmt.Errorf("creating new database failed: %w", err)
	}

	logging.Info("Initialising database schema")
	if err := database.InitialiseSchema(ctx); err != nil {
		return fmt.Errorf("initialising database schema: %w", err)
	}
	logging.Info("Database schema initialised successfully")

	logging.Info("Creating ingester and querier")
	ingesterMetrics := metrics.NewIngesterMetrics()

	// Set backlog warning threshold to 80% of max backlog size if configured
	if r.config.IngestionConfig.MaxBacklogSize > 0 {
		threshold := int(float64(r.config.IngestionConfig.MaxBacklogSize) * 0.8)
		ingesterMetrics.SetBacklogWarningThreshold(threshold)
	}

	jobIngester, err := ingester.NewIngester(r.config.IngestionConfig, r.config.QueueConfig, database, ingesterMetrics)
	if err != nil {
		return fmt.Errorf("creating new ingester failed: %w", err)
	}

	if err := jobIngester.Setup(ctx); err != nil {
		return fmt.Errorf("setting up ingester failed: %w", err)
	}

	querierMetrics := metrics.NewQuerierMetrics()

	// Set max errors from config if specified
	if r.config.QueryConfig.MaxErrorsToCollect > 0 {
		querierMetrics.SetMaxErrors(r.config.QueryConfig.MaxErrorsToCollect)
	}

	querier := querier.NewQuerier(r.config.QueryConfig, r.config.QueueConfig, database, querierMetrics, time.Now(), r.config.TestDuration)

	actorMetrics := metrics.NewActorMetrics()
	actor := actions.NewActor(
		r.config.ActionsConfig,
		r.config.QueueConfig,
		database,
		actorMetrics,
		time.Now(),
	)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	testStart := time.Now()
	var metricsStart time.Time

	logging.Info("Starting ingester")
	logging.Infof("Configured for %d submissions per hour across %d workers",
		r.config.IngestionConfig.SubmissionsPerHour,
		r.config.IngestionConfig.NumWorkers)
	wg.Go(func() { jobIngester.Run(runCtx) })

	logging.Info("Starting querier")
	logging.Infof("Configured for %d GetJobs queries/hour and %d GetJobGroups queries/hour",
		r.config.QueryConfig.GetJobsQueriesPerHour,
		r.config.QueryConfig.GetJobGroupsQueriesPerHour)
	wg.Go(func() { querier.Run(runCtx) })

	logging.Info("Starting actor")
	logging.Infof("Configured for %d reprioritisations and %d cancellations",
		len(r.config.ActionsConfig.JobSetReprioritisations),
		len(r.config.ActionsConfig.JobSetCancellations))
	wg.Go(func() { actor.Run(runCtx) })

	// Start periodic progress logging
	wg.Go(func() { r.logProgress(runCtx, ingesterMetrics, querierMetrics, actorMetrics) })

	if r.config.WarmupDuration > 0 {
		logging.Infof("Warmup period started (%v)", r.config.WarmupDuration)
		select {
		case <-time.After(r.config.WarmupDuration):
			logging.Info("Warmup period complete, resetting metrics")
			ingesterMetrics.Reset()
			querierMetrics.Reset()
			actorMetrics.Reset()
			metricsStart = time.Now()
			logging.Infof("Test measurement period started (duration: %v)", r.config.TestDuration-r.config.WarmupDuration)
		case <-ctx.Done():
			wg.Wait()
			r.tearDown(database)
			return ctx.Err()
		}
	} else {
		metricsStart = testStart
		logging.Infof("Test started (duration: %v)", r.config.TestDuration)
	}

	// Wait for the test duration, then cancel the context to signal completion
	select {
	case <-time.After(r.config.TestDuration):
		cancel()
	case <-ctx.Done():
		cancel()
		wg.Wait()
		r.tearDown(database)
		return ctx.Err()
	}

	wg.Wait()
	logging.Info("Test complete, collecting metrics")

	actualTestDuration := time.Since(metricsStart)

	r.tearDown(database)

	logging.Info("Generating metrics reports")
	ingesterReport := ingesterMetrics.GenerateReport()
	querierReport := querierMetrics.GenerateReport()
	actorReport := actorMetrics.GenerateReport()

	testResult := metrics.BuildTestResult(r.config, ingesterReport, querierReport, actorReport, actualTestDuration)

	resultsDir := r.resultsDir
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		return fmt.Errorf("creating results directory: %w", err)
	}

	outputFilename := fmt.Sprintf("broadside-result-%s.json", time.Now().Format("20060102-150405"))
	outputPath := filepath.Join(resultsDir, outputFilename)
	if err := metrics.WriteTestResultToFile(testResult, outputPath); err != nil {
		return fmt.Errorf("writing test result to file: %w", err)
	}

	logging.Infof("Test results written to: %s", outputPath)
	logging.Info("Broadside load test completed successfully")

	return nil
}

// logProgress periodically logs test progress and metrics.
func (r *Runner) logProgress(ctx context.Context, ingesterMetrics *metrics.IngesterMetrics, querierMetrics *metrics.QuerierMetrics, actorMetrics *metrics.ActorMetrics) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ingesterReport := ingesterMetrics.GenerateReport()
			querierReport := querierMetrics.GenerateReport()
			actorReport := actorMetrics.GenerateReport()

			logging.WithFields(map[string]any{
				"batches_executed":    ingesterReport.TotalBatchesExecuted,
				"batches_failed":      ingesterReport.TotalBatchesFailed,
				"queries_executed":    ingesterReport.TotalQueriesExecuted,
				"peak_backlog":        ingesterReport.PeakBacklogSize,
				"avg_backlog":         fmt.Sprintf("%.1f", ingesterReport.AverageBacklogSize),
				"p95_batch_latency":   ingesterReport.P95ExecutionLatency,
				"querier_queries":     querierReport.TotalQueriesExecuted,
				"querier_failed":      querierReport.TotalQueriesFailed,
				"querier_error_count": len(querierReport.Errors),
				"reprioritisations":   actorReport.TotalReprioritisations,
				"cancellations":       actorReport.TotalCancellations,
				"jobs_reprioritised":  actorReport.TotalJobsReprioritised,
				"jobs_cancelled":      actorReport.TotalJobsCancelled,
			}).Info("Test progress update")
		}
	}
}

func (r *Runner) tearDown(database db.Database) {
	logging.Info("Tearing down database")
	tearDownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := database.TearDown(tearDownCtx); err != nil {
		logging.WithError(err).Warn("Failed to tear down database")
	} else {
		logging.Info("Database torn down successfully")
	}
	database.Close()
}

func (r *Runner) newDatabase() (db.Database, error) {
	return NewDatabase(r.config)
}

// NewDatabase creates a database instance based on the configuration.
// Exactly one database backend must be configured.
func NewDatabase(config configuration.TestConfig) (db.Database, error) {
	switch {
	case len(config.DatabaseConfig.Postgres) > 0:
		return db.NewPostgresDatabase(config.DatabaseConfig.Postgres), nil
	case len(config.DatabaseConfig.ClickHouse) > 0:
		return db.NewClickHouseDatabase(config.DatabaseConfig.ClickHouse), nil
	case config.DatabaseConfig.InMemory:
		return db.NewMemoryDatabase(), nil
	default:
		return nil, fmt.Errorf("no database backend configured")
	}
}
