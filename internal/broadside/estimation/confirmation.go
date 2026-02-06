package estimation

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const (
	maxJobsThreshold     = 100_000
	maxDBSizeThreshold   = 1024 * 1024 * 1024 // 1GB
	maxDurationThreshold = time.Hour
)

var p = message.NewPrinter(language.English)

func ShouldPrompt(est Estimation) bool {
	return est.TotalJobs > maxJobsThreshold ||
		est.EstimatedDatabaseSizeBytes > maxDBSizeThreshold ||
		est.TestDuration > maxDurationThreshold
}

func DisplayEstimationAndConfirm(est Estimation) (bool, error) {
	p.Println("=================================================================")
	p.Println("Broadside Load Test Estimation")
	p.Println("=================================================================")
	p.Printf("Historical jobs:       %d\n", est.HistoricalJobs)
	p.Printf("New jobs during test:  %d\n", est.NewJobs)
	p.Printf("Total jobs:            %d\n", est.TotalJobs)
	p.Printf("Estimated DB size:     %s\n", FormatBytes(est.EstimatedDatabaseSizeBytes))
	p.Printf("Total queries:         %d\n", est.TotalQueryCount)
	p.Printf("Test duration:         %s\n", est.TestDuration)
	p.Println("=================================================================")
	p.Println()
	p.Print("This test will generate significant data. Proceed? (y/N): ")

	reader := bufio.NewReader(os.Stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false, fmt.Errorf("reading user input: %w", err)
	}

	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes", nil
}
