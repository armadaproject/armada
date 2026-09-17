package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/orchestrate"
	"github.com/armadaproject/armada/internal/regatta/submit"
	"github.com/armadaproject/armada/pkg/client"
)

func init() {
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
alongside one. See cmd/regatta/config/multi-cluster.example.yaml and fakeexecutor.example.yaml.`,
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

		teardown, err := orchestrate.Setup(ctx, scenario, apiConnectionDetails)
		if err != nil {
			log.Errorf("setup failed: %s", err)
			os.Exit(1)
		}
		defer teardown(context.Background())

		spec := submit.FromLoadConfig(scenario.Load)
		if err := submit.Run(ctx, apiConnectionDetails, spec); err != nil {
			log.Errorf("run failed: %s", err)
			os.Exit(1)
		}
		log.Info("run complete")
	},
}
