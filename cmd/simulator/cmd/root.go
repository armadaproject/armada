package cmd

import (
	"math"
	"os"
	"runtime/pprof"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
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
	cmd.Flags().String("cycleStatsOutputFilePath", "", "Path of file to write cycle stats to.")
	cmd.Flags().Bool("enableFastForward", false, "Skips schedule events when we're in a steady state")
	cmd.Flags().Int("hardTerminationMinutes", math.MaxInt, "Limit the time simulated")
	cmd.Flags().Int("schedulerCyclePeriodSeconds", 10, "How often we should trigger schedule events")
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
	eventsOutputFilePath, err := cmd.Flags().GetString("eventsOutputFilePath")
	if err != nil {
		return err
	}
	cycleStatsOutputFilePath, err := cmd.Flags().GetString("cycleStatsOutputFilePath")
	if err != nil {
		return err
	}

	enableFastForward, err := cmd.Flags().GetBool("enableFastForward")
	if err != nil {
		return err
	}
	hardTerminationMinutes, err := cmd.Flags().GetInt("hardTerminationMinutes")
	if err != nil {
		return err
	}
	schedulerCyclePeriodSeconds, err := cmd.Flags().GetInt("schedulerCyclePeriodSeconds")
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
	if len(clusterSpecs)*len(workloadSpecs)*len(schedulingConfigsByFilePath) > 1 && eventsOutputFilePath != "" {
		return errors.Errorf("cannot save multiple simulations to eventsOutputFile")
	}

	ctx := armadacontext.Background()
	ctx.Info("Armada simulator")
	ctx.Infof("ClusterSpecs: %v", slices.Map(clusterSpecs, func(clusperSpec *simulator.ClusterSpec) string { return clusperSpec.Name }))
	ctx.Infof("WorkloadSpecs: %v", slices.Map(workloadSpecs, func(workloadSpec *simulator.WorkloadSpec) string { return workloadSpec.Name }))
	ctx.Infof("SchedulingConfigs: %v", maps.Keys(schedulingConfigsByFilePath))

	var fileWriter *simulator.Writer
	eventsOutputFile, err := os.Create(eventsOutputFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if err = eventsOutputFile.Close(); err != nil {
			ctx.Errorf("failed to close eventsOutputFile: %s", err)
			return
		}
	}()

	var statsWriter *simulator.StatsWriter
	cycleStatsOutputFile, err := os.Create(cycleStatsOutputFilePath)
	if err != nil {
		return err
	}
	defer func() {
		if err = cycleStatsOutputFile.Close(); err != nil {
			ctx.Errorf("failed to close cycleStatsOutputFile: %s", err)
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
				if s, err := simulator.NewSimulator(clusterSpec, workloadSpec, schedulingConfig, enableFastForward, hardTerminationMinutes, schedulerCyclePeriodSeconds); err != nil {
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

					if eventsOutputFilePath != "" {
						fw, err := simulator.NewWriter(eventsOutputFile, s.StateTransitions())
						if err != nil {
							return errors.WithStack(err)
						}
						fileWriter = fw
					}
					if cycleStatsOutputFilePath != "" {
						sw, err := simulator.NewStatsWriter(cycleStatsOutputFile, s.CycleMetrics())
						if err != nil {
							return errors.WithStack(err)
						}
						statsWriter = sw
					}
					stateTransitionChannels = append(stateTransitionChannels, s.StateTransitions())
					schedulingConfigPaths = append(schedulingConfigPaths, schedulingConfigPath)
				}
			}
		}
	}
	f, err := os.Create("profile")
	if err != nil {
		log.Fatal(err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Fatal(err)
	}
	defer pprof.StopCPUProfile()

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

	// Run eventsOutputFile writer
	g.Go(func() error {
		return fileWriter.Run(ctx)
	})

	// Run stats writer
	g.Go(func() error {
		return statsWriter.Run(ctx)
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
