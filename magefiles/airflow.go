package main

import (
	"fmt"

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

	err := sh.Run("mkdir", "-p", "localdev/airflow/opt")
	if err != nil {
		return err
	}

	// Copy aramda python packages to be used within airflow docker image.
	err = sh.Run("cp", "-r", "client/python", "localdev/airflow/opt/client")
	if err != nil {
		return err
	}

	err = sh.Run("cp", "-r", "third_party/airflow", "localdev/airflow/opt/airflow")
	if err != nil {
		return err
	}

	// Arise
	return dockerRun("compose", "--project-directory", "./localdev/airflow/",
		"up", "--build", "--always-recreate-deps", "-d")
}

func stopAirflow() error {
	fmt.Println("Stopping airflow...")

	err := dockerRun("compose", "-f", "localdev/airflow/docker-compose.yaml", "down")
	if err != nil {
		return err
	}

	return sh.Run("rm", "-rf", "localdev/airflow/opt/")
}
