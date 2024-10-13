package cmd

import (
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/simulator"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "Simulate",
		Short: "Simulate running jobs on Armada.",
		RunE:  runSimulations,
	}
	cmd.Flags().String("cluster", "", "Path specifying cluster configurations to simulate.")
	cmd.Flags().String("workload", "", "Path specifying workloads to simulate.")
	cmd.Flags().String("config", "", "Path to scheduler configurations to simulate. Uses a default config if not provided.")
	cmd.Flags().Bool("showSchedulerLogs", false, "Show scheduler logs.")
	cmd.Flags().String("outputDir", "", "Path to directory where output files will be written.  Defaults to timestamped directory.")
	cmd.Flags().Bool("overwriteOutputDir", false, "Overwrite output director if it already exists.  If false then an error will be thrown if the directory already exists")
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
	outputDirPath, err := cmd.Flags().GetString("outputDir")
	if err != nil {
		return err
	}
	overwriteDirIfExists, err := cmd.Flags().GetBool("overwriteOutputDir")
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

	if outputDirPath == "" {
		outputDirPath = fmt.Sprintf("armada_simulator_%s", time.Now().Format("2006_01_02_15_04_05"))
	}

	if pathExists(outputDirPath) && overwriteDirIfExists {
		err := os.Remove(outputDirPath)
		if err != nil {
			return err
		}
	} else if pathExists(outputDirPath) {
		return fmt.Errorf("output directory %s already exists and overwriteOutputDir not set", outputDirPath)
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

	s, err := simulator.NewSimulator(clusterSpec, workloadSpec, schedulingConfig, enableFastForward, hardTerminationMinutes, schedulerCyclePeriodSeconds)
	if err != nil {
		return err
	}

	if !showSchedulerLogs {
		s.SuppressSchedulerLogs = true
	} else {
		ctx.Info("Showing scheduler logs")
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

	// Wait for simulations to complete.
	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}
