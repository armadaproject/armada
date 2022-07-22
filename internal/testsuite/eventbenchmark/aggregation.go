package eventbenchmark

// AggregateTestBenchmarkReports aggregates all test file benchmark reports and creates a global test benchmark report.
func AggregateTestBenchmarkReports(reports []*TestCaseBenchmarkReport) *GlobalBenchmarkReport {
	var aggregatedSummary []*EventDurationsByJobId
	for _, r := range reports {
		aggregatedSummary = append(aggregatedSummary, r.Summary...)
	}
	return &GlobalBenchmarkReport{
		Statistics: calculateStatistics(aggregatedSummary),
		Subreports: reports,
	}
}
