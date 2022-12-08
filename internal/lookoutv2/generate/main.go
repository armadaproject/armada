package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

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
	swaggerGenDirFull := filepath.Join(cwd, swaggerGenDir)
	swaggerFilePathFull := filepath.Join(cwd, swaggerFilePath)
	fmt.Println(swaggerGenDirFull)
	fmt.Println(swaggerFilePathFull)
	err = os.RemoveAll(swaggerGenDirFull)
	if err != nil {
		return err
	}
	err = os.Mkdir(swaggerGenDirFull, 0o755)
	if err != nil {
		return err
	}

	executable := "swagger"
	args := []string{"generate", "server", "-t", swaggerGenDirFull, "-f", swaggerFilePathFull, "--exclude-main", "-A", "lookout"}
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
	err := cmd.Run()
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
