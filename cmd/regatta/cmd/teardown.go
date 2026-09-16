package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/kwok"
)

func init() {
	teardownCmd.Flags().String("kubeconfig", "", "path to a kubeconfig file (default: KUBECONFIG env var, then $HOME/.kube/config)")
	rootCmd.AddCommand(teardownCmd)
}

var teardownCmd = &cobra.Command{
	Use:   "teardown",
	Short: "Remove regatta's KWOK fake nodes and controller from a cluster, independent of any regatta file",
	Long: `Remove regatta's KWOK fake nodes and controller from a cluster.

This is the manual escape hatch for when a regatta run's own teardown didn't happen - a crash,
a killed process, or a machine reboot. It only touches nodes tagged kwok.x-k8s.io/node=fake, so
real nodes are never affected. Safe to run even if there's nothing to tear down.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		kubeconfigPath, err := cmd.Flags().GetString("kubeconfig")
		if err != nil {
			log.Errorf("reading --kubeconfig flag: %s", err)
			os.Exit(1)
		}

		kubeClient, err := kwok.NewClientset(kubeconfigPath)
		if err != nil {
			log.Errorf("could not build kubernetes client: %s", err)
			os.Exit(1)
		}

		log.Info("tearing down KWOK fake nodes")
		if err := kwok.Teardown(context.Background(), kubeClient); err != nil {
			log.Errorf("teardown failed: %s", err)
			os.Exit(1)
		}
		log.Info("teardown complete")
	},
}
