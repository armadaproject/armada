package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/internal/regatta/orchestrate"
)

func init() {
	teardownCmd.Flags().String("kubeconfig", "", "path to a kubeconfig file (default: KUBECONFIG env var, then $HOME/.kube/config); ignored if a scenario file is given")
	teardownCmd.Flags().String("name", "cluster-0", "execution target name the run used (see executionTargets[].name in the scenario file); identifies which kwok-controller container/kubeconfig to remove; ignored if a scenario file is given")
	rootCmd.AddCommand(teardownCmd)
}

var teardownCmd = &cobra.Command{
	Use:   "teardown [path/to/scenario.yaml]",
	Short: "Remove regatta's KWOK fake nodes and controller from a cluster",
	Long: `Remove regatta's KWOK fake nodes and controller from a cluster.

regatta run never tears its own cluster targets down automatically - it just submits load and
exits, so job state/metrics can still be collected afterwards. Run this once you're done to clean
up. Given a scenario file, tears down every cluster target it declares (fake-executor targets are
skipped: stop that process manually, e.g. ps aux | grep fakeexecutor). With no scenario file,
--kubeconfig/--name identify a single target directly. Only touches nodes tagged
kwok.x-k8s.io/node=fake, so real nodes are never affected. Safe to run even if there's nothing to
tear down.`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 1 {
			scenario, err := regattaconfig.LoadScenario(args[0])
			if err != nil {
				log.Errorf("loading scenario file: %s", err)
				os.Exit(1)
			}
			orchestrate.Teardown(context.Background(), scenario)
			log.Info("teardown complete")
			return
		}

		kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
		if err != nil {
			log.Errorf("reading --kubeconfig flag: %s", err)
			os.Exit(1)
		}
		targetName, err := cmd.Flags().GetString("name")
		if err != nil {
			log.Errorf("reading --name flag: %s", err)
			os.Exit(1)
		}
		kubeClient, err := kwok.NewClientset(kubeconfigPath)
		if err != nil {
			log.Errorf("could not build kubernetes client: %s", err)
			os.Exit(1)
		}

		log.Info("tearing down KWOK fake nodes")
		if err := kwok.Teardown(context.Background(), kubeClient, targetName); err != nil {
			log.Errorf("teardown failed: %s", err)
			os.Exit(1)
		}
		log.Info("teardown complete")
	},
}
