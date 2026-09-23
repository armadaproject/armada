package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/metrics"
	"github.com/armadaproject/armada/internal/regatta/orchestrate"
	"github.com/armadaproject/armada/internal/regatta/submit"
	"github.com/armadaproject/armada/pkg/client"
)

func init() {
	runCmd.Flags().String("metrics-results-path", "", "directory to write the post-run Prometheus metrics report into (overrides the scenario file's metrics.resultsPath)")
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run ./path/to/scenario.yaml",
	Short: "Assemble a benchmarking environment (KWOK fake nodes and/or fake executors) and run a submission against Armada",
	Long: `Assemble a benchmarking environment and run a submission against Armada.

A scenario file mostly points to other files - an .armadactl.yaml, kubeconfigs, node-profile
YAML files, job-spec files - rather than embedding everything inline. executionTargets may
contain any number of "cluster" targets or any number of "fake-executor" targets, but never a
mix of both: fake-executor simulates nodes in place of a real cluster, it is not a target to run
alongside one. See cmd/regatta/config/two-cluster.example.yaml and fakeexecutor.example.yaml.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		scenario, err := regattaconfig.LoadScenario(args[0])
		if err != nil {
			log.Errorf("loading scenario file: %s", err)
			os.Exit(1)
		}

		if err := client.LoadCommandlineArgsFromConfigFile(scenario.Armadactl); err != nil {
			log.Errorf("loading armadactl config: %s", err)
			os.Exit(1)
		}
		apiConnectionDetails, err := client.ExtractCommandlineArmadaApiConnectionDetails()
		if err != nil {
			log.Errorf("could not retrieve Armada API connection details: %s", err)
			os.Exit(1)
		}

		ctx, cancel := context.WithCancel(context.Background())
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			log.Info("received interrupt, cancelling run (press again to force-quit without tearing down)")
			cancel()
			<-sigCh
			log.Info("received second interrupt, force-quitting immediately")
			os.Exit(1)
		}()
		defer cancel()

		if _, err := orchestrate.Setup(ctx, scenario, apiConnectionDetails); err != nil {
			log.Errorf("setup failed: %s", err)
			os.Exit(1)
		}

		spec := submit.FromLoadConfig(scenario.Load)
		start := time.Now()
		runTimestamp := start.Format("20060102-150405")
		if err := submit.Run(ctx, apiConnectionDetails, spec); err != nil {
			log.Errorf("run failed: %s", err)
			os.Exit(1)
		}

		resultsPath, err := cmd.Flags().GetString("metrics-results-path")
		if err != nil {
			log.Errorf("reading --metrics-results-path flag: %s", err)
			os.Exit(1)
		}
		if resultsPath == "" {
			resultsPath = scenario.MetricsResultsDir()
		}
		log.Infof("waiting for queue %q to drain before collecting metrics...", spec.Queue)
		end := metrics.WaitForQueueDrain(ctx, scenario.PrometheusURL(), spec.Queue)

		log.Infof("waiting %s for Prometheus to catch up before collecting metrics...", scenario.Metrics.PostRunDelayDuration)
		time.Sleep(scenario.Metrics.PostRunDelayDuration)

		report, err := metrics.Collect(ctx, scenario.PrometheusURL(), spec.Queue, start, end)
		if err != nil {
			log.Errorf("collecting metrics report: %s", err)
		} else {
			report.Scenario = scenario
			outputFilename := fmt.Sprintf("regatta-result-%s.json", runTimestamp)
			outputPath := filepath.Join(resultsPath, outputFilename)
			if err := report.WriteJSON(outputPath); err != nil {
				log.Errorf("writing metrics report: %s", err)
			} else {
				log.Infof("metrics report written to %s", outputPath)
			}
		}

		log.Info("run complete - nothing was torn down: tear down cluster targets with " +
			"`regatta teardown`, and stop any fake-executor process(es) manually")
	},
}
