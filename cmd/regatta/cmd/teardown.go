package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/internal/regatta/orchestrate"
)

func init() {
	teardownCmd.Flags().String("kubeconfig", "", "path to a kubeconfig file (default: KUBECONFIG env var, then $HOME/.kube/config); ignored if a scenario file is given")
	teardownCmd.Flags().String("name", "", "execution target name the run used (see executionTargets[].name in the scenario file); identifies which kwok-controller container/kubeconfig to remove; required if no scenario file is given")
	rootCmd.AddCommand(teardownCmd)
}

var teardownCmd = &cobra.Command{
	Use:   "teardown ./path/to/scenario.yaml",
	Short: "Remove regatta's KWOK fake nodes and controller from a cluster",
	Long: `Remove regatta's KWOK fake nodes and controller from a cluster.

regatta run never tears its own cluster targets down automatically - it just submits load and
exits, so job state/metrics can still be collected afterwards. Run this once you're done to clean
up, passing the same scenario file you ran, e.g.:

  go run ./cmd/regatta teardown cmd/regatta/config/scenarios/two-cluster.example.yaml

This tears down every cluster target the scenario file declares. To tear down a single target by
hand instead of via a scenario file, omit the scenario file and use --kubeconfig/--name to
identify it directly, e.g.:

  go run ./cmd/regatta teardown --kubeconfig .kube/external/multicluster/cluster-1 --name gpu-cluster

Only touches nodes tagged kwok.x-k8s.io/node=fake, so real nodes are never affected. Safe to run
even if there's nothing to tear down.`,
	Args: cobra.MaximumNArgs(1),
	RunE: teardownCmdRunE,
}

func teardownCmdRunE(cmd *cobra.Command, args []string) error {
	if len(args) == 1 {
		// Only the targets are needed, so the job-spec files the scenario names may be gone.
		scenario, err := regattaconfig.LoadScenarioTargets(args[0])
		if err != nil {
			return fmt.Errorf("loading scenario file: %w", err)
		}
		if err := orchestrate.Teardown(context.Background(), scenario); err != nil {
			return fmt.Errorf("teardown did not finish: %w", err)
		}
		log.Info("teardown complete")
		return nil
	}

	targetName, err := cmd.Flags().GetString("name")
	if err != nil {
		return fmt.Errorf("reading --name flag: %w", err)
	}
	if targetName == "" {
		_ = cmd.Help()
		return errors.New("either a scenario file or --name must be given")
	}
	kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
	if err != nil {
		return fmt.Errorf("reading --kubeconfig flag: %w", err)
	}
	kubeClient, err := kwok.NewClientset(kubeconfigPath, regattaconfig.KubernetesClientConfiguration{})
	if err != nil {
		return fmt.Errorf("could not build kubernetes client: %w", err)
	}

	log.Info("tearing down KWOK fake nodes")
	if err := kwok.Teardown(context.Background(), kubeClient, targetName, regattaconfig.DefaultNodeConcurrency); err != nil {
		return fmt.Errorf("teardown failed: %w", err)
	}
	log.Info("teardown complete")
	return nil
}
