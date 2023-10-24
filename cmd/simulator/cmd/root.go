package cmd

import (
	"fmt"
	log2 "log"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "Simulate",
		Short: "Simulate the scheduling of jobs on Armada",
		RunE:  runSimulations,
	}

	cmd.Flags().BoolP("verbose", "v", false, "Logs detailed output to console when specified")
	cmd.Flags().String("clusters", "", "Pattern specifying cluster configurations to simulate on")
	cmd.Flags().String("workloads", "", "Pattern specifying workloads to simulate")
	cmd.Flags().String("configs", "", "Pattern specifying scheduler configurations to use for simulation")

	return cmd
}

func runSimulations(cmd *cobra.Command, args []string) error {
	clusterPattern, err := cmd.Flags().GetString("clusters")
	if err != nil {
		return err
	}
	workloadPattern, err := cmd.Flags().GetString("workloads")
	if err != nil {
		return err
	}
	configPattern, err := cmd.Flags().GetString("configs")
	if err != nil {
		return err
	}
	verbose, err := cmd.Flags().GetBool("verbose")
	if err != nil {
		return err
	}

	ctx := armadacontext.Background()
	if !verbose {
		logger := logrus.New()
		logger.SetLevel(logrus.ErrorLevel)
		ctx = armadacontext.New(ctx, logrus.NewEntry(logger))
	}

	clusterSpecs, err := simulator.ClusterSpecsFromPattern(clusterPattern)
	if err != nil {
		return err
	}
	workloadSpecs, err := simulator.WorkloadFromPattern(workloadPattern)
	if err != nil {
		return err
	}
	schedulingConfigsByFilePath, err := simulator.SchedulingConfigsByFilePathFromPattern(configPattern)
	if err != nil {
		return err
	}

	simulators := make([]*simulator.Simulator, 0)
	metricsCollectors := make([]*simulator.MetricsCollector, 0)
	eventSequenceChannels := make([]<-chan *armadaevents.EventSequence, 0)
	filePaths := make([]string, 0)

	for _, clusterSpec := range clusterSpecs {
		for _, workloadSpec := range workloadSpecs {
			for filePath, schedulingConfig := range schedulingConfigsByFilePath {
				if s, err := simulator.NewSimulator(clusterSpec, workloadSpec, schedulingConfig); err != nil {
					return err
				} else {
					simulators = append(simulators, s)
					metricsCollectors = append(metricsCollectors, simulator.NewMetricsCollector(s.Output()))
					filePaths = append(filePaths, filePath)
					eventSequenceChannels = append(eventSequenceChannels, s.Output())
				}
			}
		}
	}

	threadSafeLogger := log2.New(os.Stdout, "", 0)
	g, ctx := armadacontext.ErrGroup(ctx)
	for _, s := range simulators {
		s := s
		g.Go(func() error {
			return s.Run(ctx)
		})
	}
	for _, c := range eventSequenceChannels {
		c := c
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case eventSequence, ok := <-c:
					if !ok {
						return nil
					}
					ctx.Info(*eventSequence.Events[0].Created, simulator.EventSequenceSummary(eventSequence))
				}
			}
		})
	}
	for i, mc := range metricsCollectors {
		mc := mc
		s := simulators[i]
		fp := filePaths[i]
		g.Go(func() error {
			if err := mc.Run(ctx); err != nil {
				return err
			}

			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("\nRunning Simulation of workload %s on cluster %s with configuration %s\n", s.WorkloadSpec.Name, s.ClusterSpec.Name, fp))
			sb.WriteString(fmt.Sprint("Simulation Result: ", mc.String(), "\n"))
			threadSafeLogger.Print(sb.String())
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
