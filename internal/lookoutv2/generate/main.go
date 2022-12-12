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
)

func generateSwagger() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	swaggerGenDirFull := filepath.Join(cwd, swaggerGenDir)
	swaggerFilePathFull := filepath.Join(cwd, swaggerFilePath)
	err = os.MkdirAll(swaggerGenDirFull, 0o755)
	if err != nil {
		return err
	}

	executable := "swagger"
	args := []string{"generate", "server", "-t", swaggerGenDirFull, "-f", swaggerFilePathFull, "--exclude-main", "-A", "lookout"}
	return run(executable, args...)
}

func run(executable string, args ...string) error {
	cmd := exec.Command(executable, args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		fmt.Println("OUTPUT:")
		fmt.Println(string(out))
	}
	return err
}

func main() {
	err := generateSwagger()
	if err != nil {
		log.WithError(err).Error("swagger generation failed")
		os.Exit(1)
	}
}
