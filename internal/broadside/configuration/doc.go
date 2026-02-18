/*
Package configuration defines the input configuration for the Broadside load tester.

Broadside is a load testing tool for Armada's Lookout component, designed to simulate
production-like load on the Lookout ingestion and query pipeline. It supports multiple
database backends (Postgres, ClickHouse) and provides configurable load profiles for
job submissions, state transitions, and query patterns.

# Configuration Structure

The main configuration type is TestConfig, which defines:

  - Test duration and warmup period
  - Database configuration (Postgres or ClickHouse connection parameters)
  - Queue configuration (queue/jobset distribution and historical job setup)
  - Ingestion configuration (job submission rates and state transitions)
  - Query configuration (rates for different query types)
  - Actions configuration (scheduled reprioritisations and cancellations)

# Example YAML Configuration

	testDuration: 1h
	warmupDuration: 5m
	databaseConfig:
	  postgres:
	    host: localhost
	    port: "5432"
	    database: lookout
	queueConfig:
	  - name: queue-a
	    proportion: 0.7
	    jobSetConfig:
	      - name: jobset-1
	        proportion: 0.6
	        historicalJobsConfig:
	          numberOfJobs: 1000
	          proportionSucceeded: 0.8
	          proportionErrored: 0.1
	          proportionCancelled: 0.05
	          proportionPreempted: 0.05
	ingestionConfig:
	  batchSize: 100
	  numWorkers: 4
	  submissionsPerHour: 5000
	  jobStateTransitionConfig:
	    queueingDuration: 30s
	    pendingDuration: 10s
	    leasedDuration: 5s
	    proportionSucceed: 0.9
	    runningToSuccessDuration: 5m
	    proportionFail: 0.1
	    runningToFailureDuration: 2m
	queryConfig:
	  getJobsQueriesPerHour: 100
	  getJobGroupsQueriesPerHour: 50
	actionsConfig:
	  jobSetReprioritisations:
	    - performAfterTestStartTime: 30m
	      queue: queue-a
	      jobSet: jobset-1

All configuration types are designed to be unmarshallable from YAML using standard
YAML struct tags.

# Validation

Each configuration struct has a Validate() method that performs comprehensive validation:

  - TestConfig.Validate() validates the entire configuration tree
  - Checks all durations are non-negative or positive as required
  - Ensures proportions are in valid ranges and sum correctly (with tolerance for floating point)
  - Validates database configuration has exactly one backend configured
  - Ensures scheduled actions occur within the test duration
  - Validates all string fields that must not be empty

Example usage:

	var config TestConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
	    return err
	}
	if err := config.Validate(); err != nil {
	    return fmt.Errorf("invalid configuration: %w", err)
	}
*/

package configuration
