package main

// RunSimulator runs the simulator with specified configurations.
func SimulatorFull() error {
	args := []string{
		"run",
		"./cmd/simulator/main.go",
		"--clusters", "./internal/scheduler/simulator/testdata/clusters/*.yaml",
		"--workloads", "./internal/scheduler/simulator/testdata/workloads/**/*.yaml",
		"--configs", "./internal/scheduler/simulator/testdata/configs/defaultSchedulingConfig.yaml",
	}

	return goRun(args...)
}
