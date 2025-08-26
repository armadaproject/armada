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
	"github.com/armadaproject/armada/internal/queryingester/configuration"
	"github.com/armadaproject/armada/internal/queryingester/dbloadtester"
)

func init() {
	pflag.StringSlice(
		"queryIngesterConfig",
		[]string{},
		"path to the configuration for the query ingester under test",
	)
	pflag.Parse()
}

const ReportTemplate string = `
	Load Test on Query Ingester at %s

	Configuration:
		Total Jobs Simulated: %d
		Total Concurrent Jobs Simulated: %d
		Maximum Batch of Jobs Per Queue: %d
		Queues in Use: %s
		QueryIngester Config:

%s

	Results:
		Total Load Test Duration: %s
		Total DB Insertion Duration: %s
		Number of Events Processed: %d
		Average DB Insertion Time Per Event: %f milliseconds
		Events Processed By DB Per Second: %f events
`

func main() {
	log.MustConfigureApplicationLogging()
	common.BindCommandlineArguments()

	var config configuration.QueryIngesterConfig
	userSpecifiedConfigs := viper.GetStringSlice("queryIngesterConfig")
	common.LoadConfig(&config, "./config/queryingester", userSpecifiedConfigs)

	defaultQueues := make([]string, 10)
	for i := range defaultQueues {
		defaultQueues[i] = fmt.Sprintf("queue-%d", i)
	}

	loadtesterConfig := dbloadtester.Config{
		TotalJobs:            200000,
		TotalConcurrentJobs:  20000,
		QueueSubmitBatchSize: 300,
		QueueNames:           defaultQueues,
		JobTemplateFile:      "internal/queryingester/dbloadtester/test_data.yaml",
	}

	loadtester := dbloadtester.Setup(
		config,
		loadtesterConfig,
	)

	results, err := loadtester.Run(app.CreateContextWithShutdown())
	if err != nil {
		log.Errorf("Ingestion simulator failed: %v", err)
	}

	QConfig, err := yaml.Marshal(config)
	if err != nil {
		log.Warn("Failed to marshal query ingester config for report output")
	}
	log.Infof(
		ReportTemplate,
		time.Now().Format("2006-01-02"),
		loadtesterConfig.TotalJobs,
		loadtesterConfig.TotalConcurrentJobs,
		loadtesterConfig.QueueSubmitBatchSize,
		loadtesterConfig.QueueNames,
		string(QConfig),
		results.TotalTestDuration,
		results.TotalDBInsertionDuration,
		results.TotalEventsProcessed,
		float64(results.TotalDBInsertionDuration.Milliseconds())/float64(results.TotalEventsProcessed),
		float64(results.TotalEventsProcessed)/float64(results.TotalDBInsertionDuration.Seconds()),
	)
}
