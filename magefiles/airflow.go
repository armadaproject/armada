package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/sh"
)

// Start or stop a localdev integrated airflow instance via docker-compose.
func Airflow(arg string) error {
	err := validateArg(arg, []string{"start", "stop", "restart"})
	if err != nil {
		return err
	}

	switch arg {
	case "start":
		err = startAirflow()
	case "stop":
		err = stopAirflow()
	case "restart":
		err = stopAirflow()
		if err != nil {
			return err
		}
		err = startAirflow()
	}

	if err != nil {
		return err
	}

	fmt.Println("Done.")

	return nil
}

func startAirflow() error {
	fmt.Println("Starting airflow...")

	err := sh.Run("mkdir", "-p", "developer/airflow/opt")
	if err != nil {
		return err
	}

	// Copy aramda python packages to be used within airflow docker image.
	err = sh.Run("cp", "-r", "client/python", "developer/airflow/opt/client")
	if err != nil {
		return err
	}

	err = sh.Run("cp", "-r", "third_party/airflow", "developer/airflow/opt/airflow")
	if err != nil {
		return err
	}

	// Arise
	return dockerRun("compose", "--project-directory", "./developer/airflow/",
		"up", "--build", "--always-recreate-deps", "-d")
}

func stopAirflow() error {
	fmt.Println("Stopping airflow...")

	err := dockerRun("compose", "-f", "developer/airflow/docker-compose.yaml", "down")
	if err != nil {
		return err
	}

	return sh.Run("rm", "-rf", "developer/airflow/opt/")
}

// AirflowOperator builds the Airflow Operator
func AirflowOperator() error {
	fmt.Println("Building Airflow Operator...")

	err := os.RemoveAll("proto-airflow")
	if err != nil {
		return fmt.Errorf("failed to remove proto-airflow directory: %w", err)
	}

	err = os.MkdirAll("proto-airflow", os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create proto-airflow directory: %w", err)
	}

	err = dockerRun("buildx", "build", "-o", "type=docker", "-t", "armada-airflow-operator-builder", "-f", "./build/airflow-operator/Dockerfile", ".")
	if err != nil {
		return fmt.Errorf("failed to build Airflow Operator: %w", err)
	}

	err = dockerRun("run", "--rm", "-v", "${PWD}/proto-airflow:/proto-airflow", "-v", "${PWD}:/go/src/armada", "-w", "/go/src/armada", "armada-airflow-operator-builder", "./scripts/build-airflow-operator.sh")
	if err != nil {
		return fmt.Errorf("failed to run build-airflow-operator.sh script: %w", err)
	}

	return nil
}
