package eventbenchmark

// AggregateTestBenchmarkReports aggregates all test file benchmark reports and creates a global test benchmark report.
func AggregateTestBenchmarkReports(reportName string, reports []*TestBenchmarkReport) *TestBenchmarkReport {
	var aggregatedSummary []*EventDurationsByJobId
	for _, r := range reports {
		aggregatedSummary = append(aggregatedSummary, r.Summary...)
	}
	return &TestBenchmarkReport{
		Name:       reportName,
		Statistics: calculateStatistics(aggregatedSummary),
		Subreports: reports,
	}
}
