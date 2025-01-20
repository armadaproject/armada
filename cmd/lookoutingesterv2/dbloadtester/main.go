package main

import (
	"fmt"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"sigs.k8s.io/yaml"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/app"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/configuration"
	"github.com/armadaproject/armada/internal/lookoutingesterv2/dbloadtester"
)

func init() {
	pflag.StringSlice(
		"lookoutIngesterConfig",
		[]string{},
		"Fully qualified path to application configuration file (for multiple config files repeat this arg or separate paths with commas)",
	)
	pflag.Parse()
}

const ReportTemplate string = `
	Load Test on LookoutIngester at %s

	Configuration:
		Total Jobs Simulated: %d
		Total Concurrent Jobs Simulated: %d
		Maximum Batch of Jobs Per Queue: %d
		Queues in Use: %s
		LookoutIngester Config:

%s

	Results:
		Total Load Test Duration: %s
		Total DB Insertion Duration: %s
		Number of Events Processed: %d
		Average DB Insertion Time Per Event: %f milliseconds
		Events Processed By DB Per Second: %f events
`

func main() {
	common.ConfigureLogging()
	common.BindCommandlineArguments()

	var config configuration.LookoutIngesterV2Configuration
	userSpecifiedConfigs := viper.GetStringSlice("lookoutIngesterConfig")
	common.LoadConfig(&config, "./config/lookoutingesterv2", userSpecifiedConfigs)

	loadtesterConfig := dbloadtester.Config{
		TotalJobs:            500000,
		TotalConcurrentJobs:  50000,
		QueueSubmitBatchSize: 300,
		QueueNames:           []string{"queue1", "queue2", "queue3"},
		JobTemplateFile:      "internal/lookoutingesterv2/dbloadtester/test_data.yaml",
	}

	loadtester := dbloadtester.Setup(
		config,
		loadtesterConfig,
	)

	results, err := loadtester.Run(app.CreateContextWithShutdown())
	if err != nil {
		log.Errorf("Ingestion simulator failed: %v", err)
	}

	LIConfig, err := yaml.Marshal(config)
	if err != nil {
		log.Warn("Failed to marshal lookout ingester config for report output")
	}
	fmt.Printf(
		ReportTemplate,
		time.Now().Format("2006-01-02"),
		loadtesterConfig.TotalJobs,
		loadtesterConfig.TotalConcurrentJobs,
		loadtesterConfig.QueueSubmitBatchSize,
		loadtesterConfig.QueueNames,
		string(LIConfig),
		results.TotalTestDuration,
		results.TotalDBInsertionDuration,
		results.TotalEventsProcessed,
		float64(results.TotalDBInsertionDuration.Milliseconds())/float64(results.TotalEventsProcessed),
		float64(results.TotalEventsProcessed)/float64(results.TotalDBInsertionDuration.Seconds()),
	)
}
