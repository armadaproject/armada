package estimation

import (
	"fmt"
	"time"

	"github.com/armadaproject/armada/internal/broadside/configuration"
)

type Estimation struct {
	HistoricalJobs             int
	NewJobs                    int
	TotalJobs                  int
	EstimatedDatabaseSizeBytes int64
	TotalQueryCount            int
	TestDuration               time.Duration
}

func Estimate(config configuration.TestConfig) Estimation {
	historicalJobs := calculateHistoricalJobs(config.QueueConfig)
	newJobs := calculateNewJobs(config.WarmupDuration, config.TestDuration, config.IngestionConfig.SubmissionsPerHour)
	totalJobs := historicalJobs + newJobs

	const avgBytesPerJob = 3700
	estimatedBytes := int64(totalJobs) * int64(avgBytesPerJob)

	totalQueries := calculateTotalQueries(config.TestDuration, config.QueryConfig)

	return Estimation{
		HistoricalJobs:             historicalJobs,
		NewJobs:                    newJobs,
		TotalJobs:                  totalJobs,
		EstimatedDatabaseSizeBytes: estimatedBytes,
		TotalQueryCount:            totalQueries,
		TestDuration:               config.TestDuration,
	}
}

func calculateHistoricalJobs(queueConfigs []configuration.QueueConfig) int {
	total := 0
	for _, queueConfig := range queueConfigs {
		for _, jobSetConfig := range queueConfig.JobSetConfig {
			total += jobSetConfig.HistoricalJobsConfig.NumberOfJobs
		}
	}
	return total
}

func calculateNewJobs(warmupDuration, testDuration time.Duration, submissionsPerHour int) int {
	totalDuration := warmupDuration + testDuration
	return int(totalDuration.Hours() * float64(submissionsPerHour))
}

func calculateTotalQueries(testDuration time.Duration, queryConfig configuration.QueryConfig) int {
	testHours := testDuration.Hours()
	totalQueriesPerHour := queryConfig.GetJobRunDebugMessageQueriesPerHour +
		queryConfig.GetJobRunErrorQueriesPerHour +
		queryConfig.GetJobSpecQueriesPerHour +
		queryConfig.GetJobsQueriesPerHour +
		queryConfig.GetJobGroupsQueriesPerHour
	return int(testHours * float64(totalQueriesPerHour))
}

func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/float64(TB))
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d bytes", bytes)
	}
}
