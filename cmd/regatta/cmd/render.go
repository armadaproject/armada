package cmd

import (
	"os"

	"github.com/spf13/cobra"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/render"
)

func init() {
	rootCmd.AddCommand(renderCmd)
}

var renderCmd = &cobra.Command{
	Use:   "render ./path/to/scenario.yaml",
	Short: "Render kind-cluster configs, executor configs, and a Procfile for a scenario's cluster targets",
	Long: `Render the mage/goreman-facing config text a scenario's cluster targets need to run.

For every "cluster"-type executionTargets[] entry, writes a kind-cluster config and an executor
config, plus one combined Procfile with one executor line per target, into .tmp/ directories
under cmd/regatta/config/armada/{kind,executor,procfiles}/. render only ever generates config
text - it does not create clusters or start processes itself. Point mage/goreman at the rendered
output to actually provision:

  mage kindRegatta cmd/regatta/config/armada/kind/.tmp
  goreman -f cmd/regatta/config/armada/procfiles/.tmp/regatta.Procfile start

Each rendered cluster's kubeconfig is written by "mage kindRegatta" to
.kube/external/regatta/<target-name> - the scenario file's executionTargets[].cluster.kubeconfig
must point there for each target to match.

The quickstart (2 clusters, checked-in files under cmd/regatta/config/armada/) needs no
rendering step - use render only when a scenario declares a different cluster-target shape than
the checked-in quickstart files provide (e.g. N != 2 clusters).`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		scenario, err := regattaconfig.LoadScenario(args[0])
		if err != nil {
			log.Errorf("loading scenario file: %s", err)
			os.Exit(1)
		}

		plan, err := render.BuildPlan(scenario)
		if err != nil {
			log.Errorf("building render plan: %s", err)
			os.Exit(1)
		}

		if err := render.Write(plan); err != nil {
			log.Errorf("rendering: %s", err)
			os.Exit(1)
		}

		log.Infof("rendered %d cluster target(s):", len(plan.Targets))
		log.Infof("  kind-cluster configs: %s/", plan.KindDir)
		log.Infof("  executor configs:     %s/", plan.ExecutorDir)
		log.Infof("  procfile:             %s", plan.ProcfilePath)
	},
}
