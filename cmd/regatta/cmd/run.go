package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"

	log "github.com/armadaproject/armada/internal/common/logging"
	regattaconfig "github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/internal/regatta/fakeexecutor"
	"github.com/armadaproject/armada/internal/regatta/kwok"
	"github.com/armadaproject/armada/internal/regatta/submit"
	"github.com/armadaproject/armada/pkg/client"
)

func init() {
	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run ./path/to/regatta.yaml",
	Short: "Assemble a benchmarking environment (KWOK fake nodes and/or a fake executor) and run a submission against Armada",
	Long: `Assemble a benchmarking environment and run a submission against Armada.

A regatta file mostly points to other files - an .armadactl.yaml, a kubeconfig, node-profile
YAML files, a submission spec - rather than embedding everything inline. Exactly one of
kwok.enabled or fakeExecutor.enabled must be true: fakeExecutor simulates nodes in place of a
real cluster, it is not a target to run alongside one. See cmd/regatta/config/kwok.example.yaml
and fakeexecutor.example.yaml.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		regattaFile, err := regattaconfig.Load(args[0])
		if err != nil {
			log.Errorf("loading regatta file: %s", err)
			os.Exit(1)
		}

		if err := client.LoadCommandlineArgsFromConfigFile(regattaFile.Armadactl); err != nil {
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

		var kwokClient kubernetes.Interface
		if regattaFile.Kwok.Enabled {
			kwokClient, err = kwok.NewClientset(regattaFile.Kubeconfig)
			if err != nil {
				log.Errorf("could not build kubernetes client: %s", err)
				os.Exit(1)
			}

			nodeGroup, err := regattaconfig.LoadNodeGroup(regattaFile.Kwok.NodeGroup)
			if err != nil {
				log.Errorf("loading kwok node group: %s", err)
				os.Exit(1)
			}

			kwokCfg := kwok.Config{
				KubeconfigPath:       regattaFile.Kubeconfig,
				KindClusterName:      regattaFile.Kwok.KindClusterName,
				StageCRDPath:         "cmd/regatta/kwok/stage-crd.yaml",
				StagesPath:           "cmd/regatta/kwok/stages.yaml",
				NodeGroup:            nodeGroup,
				ApiConnectionDetails: apiConnectionDetails,
				SchedulableProbe: kwok.ProbeConfig{
					Retries:      regattaFile.Kwok.ProbeRetries,
					InitialDelay: regattaFile.Kwok.ProbeDelayDuration,
				},
			}
			log.Info("setting up KWOK fake nodes")
			if err := kwok.Setup(ctx, kwokClient, kwokCfg); err != nil {
				log.Errorf("KWOK setup failed: %s", err)
				os.Exit(1)
			}
			defer func() {
				log.Info("tearing down KWOK fake nodes")
				if err := kwok.Teardown(context.Background(), kwokClient); err != nil {
					log.Errorf("KWOK teardown failed: %s", err)
				}
			}()
		}

		if regattaFile.FakeExecutor.Enabled {
			nodeGroup, err := regattaconfig.LoadNodeGroup(regattaFile.FakeExecutor.NodeGroup)
			if err != nil {
				log.Errorf("loading fake-executor node group: %s", err)
				os.Exit(1)
			}

			log.Info("starting armada-fakeexecutor")
			process, err := fakeexecutor.Start(apiConnectionDetails, nodeGroup, regattaFile.FakeExecutor)
			if err != nil {
				log.Errorf("starting armada-fakeexecutor failed: %s", err)
				os.Exit(1)
			}
			log.Infof("armada-fakeexecutor started, pid %d", process.PID())
			defer func() {
				log.Info("stopping armada-fakeexecutor")
				if err := process.Stop(); err != nil {
					log.Errorf("stopping armada-fakeexecutor failed: %s", err)
				}
			}()
		}

		spec, err := submit.Load(regattaFile.Submission)
		if err != nil {
			log.Errorf("loading submission spec: %s", err)
			os.Exit(1)
		}

		if err := submit.Run(ctx, apiConnectionDetails, spec); err != nil {
			log.Errorf("run failed: %s", err)
			os.Exit(1)
		}
		log.Info("run complete")
	},
}
