package main

import (
	"fmt"
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

const (
	swaggerGenDir   = "./gen"
	swaggerFilePath = "./swagger.yaml"
	statikDir       = "./schema/"
)

func generateSwagger() error {
	err := os.RemoveAll(swaggerGenDir)
	if err != nil {
		return err
	}
	err = os.Mkdir(swaggerGenDir, 0755)
	if err != nil {
		return err
	}

	executable := "swagger"
	args := []string{"generate", "server", "-t", swaggerGenDir, "-f", swaggerFilePath, "--exclude-main", "-A", "lookout"}
	return run(executable, args...)
}

func generateStatik() error {
	executable := "go"
	args := []string{
		"run",
		"github.com/rakyll/statik",
		"-dest",
		statikDir,
		"-src",
		statikDir,
		"-include=*.sql",
		"-ns=lookoutv2/sql",
		"-Z",
		"-f",
		"-m",
	}
	return run(executable, args...)
}

func run(executable string, args ...string) error {
	cmd := exec.Command(executable, args...)
	stdout, err := cmd.Output()
	if len(stdout) > 0 {
		fmt.Println(string(stdout))
	}
	if err != nil {
		return err
	}
	return nil
}

func main() {
	err1 := generateSwagger()
	err2 := generateStatik()
	if err1 != nil {
		log.WithError(err1).Error("swagger generation failed")
	}
	if err2 != nil {
		log.WithError(err2).Error("statik generation failed")
	}
}
