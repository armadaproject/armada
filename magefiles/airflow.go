package main

import (
	"fmt"

	"github.com/magefile/mage/sh"
)

// Start or stop a localdev integrated airflow instance via docker-compose.
func Airflow(arg string) error {
	err := validateArg(arg, []string{"start", "stop"})
	if err != nil {
		return err
	}

	if arg == "start" {
		fmt.Println("Starting airflow...")
		err = sh.Run("mkdir", "-p", "localdev/airflow/opt")
		err = sh.Run("cp", "-r", "client/python", "localdev/airflow/opt/armada_client")
		err = sh.Run("cp", "-r", "third_party/airflow", "localdev/airflow/opt/armada_airflow")
		err = dockerComposeRun(
			"--project-directory", "./localdev/airflow/", "up", "--build", "--always-recreate-deps", "-d")
	} else if arg == "stop" {
		fmt.Println("Stopping airflow...")
		err = dockerComposeRun("-f", "./localdev/airflow/docker-compose.yaml", "down")
	}

	if err != nil {
		return err
	}

	fmt.Println("Done.")

	return nil
}
