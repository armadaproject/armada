package cmd

import (
	"math"
	"os"
	"runtime/pprof"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "Simulate",
		Short: "Simulate running jobs on Armada.",
		RunE:  runSimulations,
	}
	// cmd.Flags().BoolP("verbose", "v", false, "Log detailed output to console.")
	cmd.Flags().String("cluster", "", "Path specifying cluster configurations to simulate.")
	cmd.Flags().String("workload", "", "Path specifying workloads to simulate.")
	cmd.Flags().String("config", "", "Path to scheduler configurations to simulate. Uses a default config if not provided.")
	cmd.Flags().Bool("showSchedulerLogs", false, "Show scheduler logs.")
	cmd.Flags().String("eventsOutputFilePath", "", "Path of file to write events to.")
	cmd.Flags().String("cycleStatsOutputFilePath", "", "Path of file to write cycle stats to.")
	cmd.Flags().Bool("enableFastForward", false, "Skips schedule events when we're in a steady state")
	cmd.Flags().Int("hardTerminationMinutes", math.MaxInt, "Limit the time simulated")
	cmd.Flags().Int("schedulerCyclePeriodSeconds", 10, "How often we should trigger schedule events")
	return cmd
}

func runSimulations(cmd *cobra.Command, args []string) error {
	// Get command-line arguments.
	clusterFile, err := cmd.Flags().GetString("clusters")
	if err != nil {
		return err
	}
	workloadFile, err := cmd.Flags().GetString("workloads")
	if err != nil {
		return err
	}
	configFile, err := cmd.Flags().GetString("config")
	if err != nil {
		return err
	}
	showSchedulerLogs, err := cmd.Flags().GetBool("showSchedulerLogs")
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
	clusterSpec, err := simulator.ClusterSpecFromFilePath(clusterFile)
	if err != nil {
		return err
	}
	workloadSpec, err := simulator.WorkloadSpecFromFilePath(workloadFile)
	if err != nil {
		return err
	}

	schedulingConfig := testfixtures.TestSchedulingConfig()
	if configFile != "" {
		schedulingConfig, err = simulator.SchedulingConfigFromFilePath(configFile)
		if err != nil {
			return err
		}
	}

	ctx := armadacontext.Background()
	ctx.Info("Armada simulator")
	ctx.Infof("ClusterSpec: %v", clusterSpec.Name)
	ctx.Infof("WorkloadSpecs: %v", workloadSpec.Name)
	ctx.Infof("SchedulingConfig: %v", configFile)

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

	s, err := simulator.NewSimulator(clusterSpec, workloadSpec, schedulingConfig, enableFastForward, hardTerminationMinutes, schedulerCyclePeriodSeconds)
	if err != nil {
		return err
	}

	if !showSchedulerLogs {
		s.SuppressSchedulerLogs = true
	} else {
		ctx.Info("Showing scheduler logs")
	}
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

	f, err := os.Create("profile")
	if err != nil {
		log.Fatal(err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		log.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	g, ctx := armadacontext.ErrGroup(ctx)
	g.Go(func() error {
		return s.Run(ctx)
	})

	// Run eventsOutputFile writer
	g.Go(func() error {
		return fileWriter.Run(ctx)
	})

	// Run stats writer
	g.Go(func() error {
		return statsWriter.Run(ctx)
	})

	// Wait for simulations to complete.
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
