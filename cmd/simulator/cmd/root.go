package cmd

import (
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "Simulate",
		Short: "Simulate running jobs on Armada.",
		RunE:  runSimulations,
	}
	// cmd.Flags().BoolP("verbose", "v", false, "Log detailed output to console.")
	cmd.Flags().String("clusters", "", "Glob pattern specifying cluster configurations to simulate.")
	cmd.Flags().String("workloads", "", "Glob pattern specifying workloads to simulate.")
	cmd.Flags().String("configs", "", "Glob pattern specifying scheduler configurations to simulate. Uses a default config if not provided.")
	cmd.Flags().Bool("showSchedulerLogs", false, "Show scheduler logs.")
	cmd.Flags().Int("logInterval", 0, "Log summary statistics every this many events. Disabled if 0.")
	cmd.Flags().String("eventsOutputFilePath", "", "Path of file to write events to.")
	return cmd
}

func runSimulations(cmd *cobra.Command, args []string) error {
	// Get command-line arguments.
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
	showSchedulerLogs, err := cmd.Flags().GetBool("showSchedulerLogs")
	if err != nil {
		return err
	}
	logInterval, err := cmd.Flags().GetInt("logInterval")
	if err != nil {
		return err
	}
	filePath, err := cmd.Flags().GetString("eventsOutputFilePath")
	if err != nil {
		return err
	}

	// Load test specs. and config.
	clusterSpecs, err := simulator.ClusterSpecsFromPattern(clusterPattern)
	if err != nil {
		return err
	}
	workloadSpecs, err := simulator.WorkloadsFromPattern(workloadPattern)
	if err != nil {
		return err
	}
	var schedulingConfigsByFilePath map[string]configuration.SchedulingConfig
	if configPattern == "" {
		// Use default test config if no pattern is provided.
		schedulingConfigsByFilePath = map[string]configuration.SchedulingConfig{
			"default": testfixtures.TestSchedulingConfig(),
		}
	} else {
		schedulingConfigsByFilePath, err = simulator.SchedulingConfigsByFilePathFromPattern(configPattern)
		if err != nil {
			return err
		}
	}
	if len(clusterSpecs)*len(workloadSpecs)*len(schedulingConfigsByFilePath) > 1 && filePath != "" {
		return errors.Errorf("cannot save multiple simulations to file")
	}

	ctx := armadacontext.Background()
	ctx.Info("Armada simulator")
	ctx.Infof("ClusterSpecs: %v", util.Map(clusterSpecs, func(clusperSpec *simulator.ClusterSpec) string { return clusperSpec.Name }))
	ctx.Infof("WorkloadSpecs: %v", util.Map(workloadSpecs, func(workloadSpec *simulator.WorkloadSpec) string { return workloadSpec.Name }))
	ctx.Infof("SchedulingConfigs: %v", maps.Keys(schedulingConfigsByFilePath))

	var fileWriter *simulator.Writer
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		if err = file.Close(); err != nil {
			ctx.Errorf("failed to close file: %s", err)
			return
		}
	}()

	// Setup a simulator for each combination of (clusterSpec, workloadSpec, schedulingConfig).
	simulators := make([]*simulator.Simulator, 0)
	metricsCollectors := make([]*simulator.MetricsCollector, 0)
	stateTransitionChannels := make([]<-chan simulator.StateTransition, 0)
	schedulingConfigPaths := make([]string, 0)
	for _, clusterSpec := range clusterSpecs {
		for _, workloadSpec := range workloadSpecs {
			for schedulingConfigPath, schedulingConfig := range schedulingConfigsByFilePath {
				if s, err := simulator.NewSimulator(clusterSpec, workloadSpec, schedulingConfig); err != nil {
					return err
				} else {
					if !showSchedulerLogs {
						s.SuppressSchedulerLogs = true
					} else {
						ctx.Info("Showing scheduler logs")
					}
					simulators = append(simulators, s)
					mc := simulator.NewMetricsCollector(s.StateTransitions())
					mc.LogSummaryInterval = logInterval
					metricsCollectors = append(metricsCollectors, mc)

					if filePath != "" {
						fw, err := simulator.NewWriter(file, s.StateTransitions())
						if err != nil {
							return errors.WithStack(err)
						}
						fileWriter = fw
					}
					stateTransitionChannels = append(stateTransitionChannels, s.StateTransitions())
					schedulingConfigPaths = append(schedulingConfigPaths, schedulingConfigPath)
				}
			}
		}
	}

	// Run simulators.
	g, ctx := armadacontext.ErrGroup(ctx)
	for _, s := range simulators {
		s := s
		g.Go(func() error {
			return s.Run(ctx)
		})
	}

	// Log events to stdout.
	for _, c := range stateTransitionChannels {
		c := c
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case stateTransition, ok := <-c:
					if !ok {
						return nil
					}
					ctx.Debug(*stateTransition.EventSequence.Events[0].Created, simulator.EventSequenceSummary(stateTransition.EventSequence))
				}
			}
		})
	}

	// Run file writer
	g.Go(func() error {
		return fileWriter.Run(ctx)
	})

	// Run metric collectors.
	for _, mc := range metricsCollectors {
		mc := mc
		g.Go(func() error {
			return mc.Run(ctx)
		})
	}

	// Wait for simulations to complete.
	if err := g.Wait(); err != nil {
		return err
	}

	// Log overall statistics.
	for i, mc := range metricsCollectors {
		s := simulators[i]
		schedulingConfigPath := schedulingConfigPaths[i]
		ctx.Infof("Simulation result")
		ctx.Infof("ClusterSpec: %s", s.ClusterSpec.Name)
		ctx.Infof("WorkloadSpec: %s", s.WorkloadSpec.Name)
		ctx.Infof("SchedulingConfig: %s", schedulingConfigPath)
		ctx.Info(mc.String())
	}

	return nil
}
