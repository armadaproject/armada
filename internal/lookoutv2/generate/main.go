package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

const (
	swaggerGenDir   = "./gen"
	swaggerFilePath = "./swagger.yaml"

	statikDir = "./schema/"
)

func generateSwagger() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	fmt.Println(cwd)
	err = os.RemoveAll(swaggerGenDir)
	if err != nil {
		return err
	}
	err = os.Mkdir(swaggerGenDir, 0o755)
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
	var outBuffer, errBuffer bytes.Buffer
	cmd := exec.Command(executable, args...)
	cmd.Stdout = &outBuffer
	cmd.Stderr = &errBuffer
	err := cmd.Run()
	if len(outBuffer.String()) > 0 {
		fmt.Println(outBuffer.String())
	}
	if len(errBuffer.String()) > 0 {
		fmt.Println(errBuffer.String())
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
	if err1 != nil || err2 != nil {
		os.Exit(1)
	}
}
