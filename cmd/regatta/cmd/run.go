package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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

// warnAboutReadinessFailures repeats each tolerated readiness failure at the end of a run, where it is seen.
func warnAboutReadinessFailures(failures []metrics.ReadinessFailure) {
	for _, failure := range failures {
		log.Warnf("target %q never passed its readiness check and the run continued anyway (cluster.continueOnReadinessFailure), so these results may include scheduler warm-up or faults: %s", failure.Target, failure.Error)
	}
}

// loadArmadaConnection loads the scenario's armadactl config and returns the connection details
// for the context to run against: the --context flag if contextFlagSet, else the scenario's
// authContext if set, else the config file's own currentContext.
func loadArmadaConnection(scenario *regattaconfig.Scenario, contextFlagSet bool, configFlag string) (*client.ApiConnectionDetails, error) {
	armadactl := scenario.Armadactl
	if configFlag != "" {
		armadactl = configFlag // an explicit --config beats the scenario's armadactl field
	}
	if err := client.LoadCommandlineArgsFromConfigFile(armadactl); err != nil {
		return nil, fmt.Errorf("loading armadactl config: %w", err)
	}
	if scenario.AuthContext != "" && !contextFlagSet {
		if err := client.SetDefaultContext(scenario.AuthContext); err != nil {
			return nil, fmt.Errorf("scenario authContext %q: %w (available: %s)", scenario.AuthContext, err, strings.Join(client.ExtractConfigurationContexts(), ", "))
		}
	}
	details, err := client.ExtractCommandlineArmadaApiConnectionDetails()
	if err != nil {
		return nil, fmt.Errorf("could not retrieve Armada API connection details: %w", err)
	}
	log.Infof("running against Armada at %s (armadactl context %q)", details.ArmadaUrl, viper.GetString("currentContext"))
	return details, nil
}

var runCmd = &cobra.Command{
	Use:   "run ./path/to/scenario.yaml",
	Short: "Assemble a benchmarking environment (KWOK fake nodes) and run a submission against Armada",
	Long: `Assemble a benchmarking environment and run a submission against Armada.

A scenario file mostly points to other files, including a .armadactl.yaml, kubeconfigs, node-profile
YAML files, job-spec files, rather than embedding everything inline. Its authContext field picks
which context of that .armadactl.yaml to use (the --context flag overrides it). executionTargets contains
any number of "cluster" targets. See cmd/regatta/config/scenarios/two-cluster.example.yaml.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		scenario, err := regattaconfig.LoadScenario(args[0])
		if err != nil {
			log.Errorf("loading scenario file: %s", err)
			os.Exit(1)
		}

		configFlag, err := cmd.Flags().GetString("config")
		if err != nil {
			log.Errorf("reading --config: %s", err)
			os.Exit(1)
		}
		apiConnectionDetails, err := loadArmadaConnection(scenario, cmd.Flags().Changed("context"), configFlag)
		if err != nil {
			log.Errorf("%s", err)
			os.Exit(1)
		}

		// Planned before anything is created, so a bad arrival fails the run at once.
		spec, err := submit.FromLoadConfig(scenario.Load)
		if err != nil {
			log.Errorf("planning submission: %s", err)
			os.Exit(1)
		}
		queues := scenario.Load.Names()

		runStart := time.Now()
		if scenario.Load.Continuous() {
			log.Infof("scenario %s: %d target(s), %d queue(s), %d bounded jobs plus continuous submission, metrics from %s",
				args[0], len(scenario.ExecutionTargets), len(queues), scenario.Load.TotalJobs(), scenario.PrometheusURL())
		} else {
			log.Infof("scenario %s: %d target(s), %d queue(s), %d jobs, metrics from %s",
				args[0], len(scenario.ExecutionTargets), len(queues), scenario.Load.TotalJobs(), scenario.PrometheusURL())
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

		toEnsure := make([]submit.QueueToEnsure, len(scenario.Load.Queues))
		for i, q := range scenario.Load.Queues {
			toEnsure[i] = submit.QueueToEnsure{Name: q.Name, PriorityFactor: q.PriorityFactor}
		}
		if err := submit.EnsureQueues(ctx, apiConnectionDetails, toEnsure); err != nil {
			log.Errorf("ensuring queues: %s", err)
			os.Exit(1)
		}

		setupStart := time.Now()
		_, readinessFailures, err := orchestrate.Setup(ctx, scenario, apiConnectionDetails)
		if err != nil {
			log.Errorf("setup failed after %s: %s", time.Since(setupStart).Round(time.Millisecond), err)
			os.Exit(1)
		}
		log.Infof("setup of %d target(s) finished in %s", len(scenario.ExecutionTargets), time.Since(setupStart).Round(time.Millisecond))

		resultsPath, err := cmd.Flags().GetString("metrics-results-path")
		if err != nil {
			log.Errorf("reading --metrics-results-path flag: %s", err)
			os.Exit(1)
		}
		if resultsPath == "" {
			resultsPath = scenario.MetricsResultsDir()
		}
		reports := reportInputs{scenario: scenario, queues: queues, readinessFailures: readinessFailures, resultsPath: resultsPath}

		start := time.Now()
		runTimestamp := start.Format("20060102-150405")

		if spec.Continuous() {
			err := runContinuously(ctx, reports, func(ctx context.Context) error {
				return submit.Run(ctx, apiConnectionDetails, spec)
			}, runTimestamp, start)
			if err != nil {
				log.Errorf("run failed: %s", err)
				os.Exit(1)
			}
			warnAboutReadinessFailures(readinessFailures)
			log.Infof("run took %s in total (setup %s, submitted for %s)",
				time.Since(runStart).Round(time.Second), start.Sub(setupStart).Round(time.Second), time.Since(start).Round(time.Second))
			log.Info("run complete - nothing was torn down: tear down cluster targets with `regatta teardown`")
			return
		}

		if err := submit.Run(ctx, apiConnectionDetails, spec); err != nil {
			log.Errorf("run failed: %s", err)
			os.Exit(1)
		}
		log.Infof("submission finished in %s", time.Since(start).Round(time.Millisecond))

		log.Infof("waiting for %d queue(s) to drain before collecting metrics...", len(queues))
		end := metrics.WaitForQueueDrain(ctx, scenario.PrometheusURL(), queues, start)
		log.Infof("%d queue(s) drained %s after submission started", len(queues), end.Sub(start).Round(time.Second))

		log.Infof("waiting %s for Prometheus to catch up before collecting metrics...", scenario.Metrics.PostRunDelayDuration)
		time.Sleep(scenario.Metrics.PostRunDelayDuration)

		reports.write(reportFileName(runTimestamp, 0, false), start, end)

		log.Infof("run took %s in total (setup %s, metrics window %s)",
			time.Since(runStart).Round(time.Second), start.Sub(setupStart).Round(time.Second), end.Sub(start).Round(time.Second))
		warnAboutReadinessFailures(readinessFailures)
		log.Info("run complete - nothing was torn down: tear down cluster targets with `regatta teardown`")
	},
}
