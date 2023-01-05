package main

import (
	"fmt"
	"os"
	"time"

	goreleaserConfig "github.com/goreleaser/goreleaser/pkg/config"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	yaml "gopkg.in/yaml.v3"

	"github.com/G-Research/armada/pkg/client/util"
)

// Build images, spin up a test environment, and run the integration tests against it.
func ciRunTests() error {
	if err := os.MkdirAll(".kube", os.ModeDir|0o755); err != nil {
		return err
	}
	err := dockerComposeRun("up", "-d", "redis", "postgres", "pulsar", "stan")
	if err != nil {
		return err
	}

	// TODO: Necessary to avoid connection error on Armada server startup.
	time.Sleep(10 * time.Second)
	err = dockerComposeRun("up", "-d", "server", "executor")
	if err != nil {
		return err
	}

	time.Sleep(5 * time.Second)
	err = goRun("run", "cmd/armadactl/main.go", "create", "queue", "e2e-test-queue")
	if err != nil {
		return err
	}
	err = goRun("run", "cmd/testsuite/main.go", "test",
		"--tests", "testsuite/testcases/basic/*",
		"--junit", "junit.xml",
	)
	if err != nil {
		return err
	}
	return nil
}

func ciMinimalRelease() error {
	return goreleaserRun("release", "--snapshot", "--rm-dist", "-f", ".goreleaser-minimal.yml")
}

// Write a minimal goreleaser config to .goreleaser-docker.yml
// containing only the subset of targets in .goreleaser.yaml necessary
// for building a set of specified Docker images.
func ciWriteMinimalReleaseConfig() error {
	// Docker targets to build and the build targets necessary to do so.
	dockerIds := map[string]bool{
		"bundle": true,
	}
	buildIds := map[string]bool{
		"server":          true,
		"executor":        true,
		"binoculars":      true,
		"lookoutingester": true,
		"eventingester":   true,
	}

	goreleaserConfigPath := "./.goreleaser.yml"
	config := goreleaserConfig.Project{}
	if err := util.BindJsonOrYaml(goreleaserConfigPath, &config); err != nil {
		return err
	}

	dockers := make([]goreleaserConfig.Docker, 0)
	for _, docker := range config.Dockers {
		if dockerIds[docker.ID] {
			dockers = append(dockers, docker)
		}
	}
	if len(dockers) == 0 {
		return errors.Errorf("%v matched no dockers in %s", dockerIds, goreleaserConfigPath)
	}

	builds := make([]goreleaserConfig.Build, 0)
	for _, build := range config.Builds {
		if buildIds[build.ID] {
			builds = append(builds, build)
		}
	}
	if len(builds) == 0 {
		return errors.Errorf("%v matched no builds in %s", buildIds, goreleaserConfigPath)
	}

	targets := make(map[string]interface{})
	for _, docker := range dockers {
		targets[fmt.Sprintf("%s_%s", docker.Goos, docker.Goarch)] = true
	}
	for i := range builds {
		builds[i].Goos = nil
		builds[i].Goarch = nil
		builds[i].Targets = maps.Keys(targets)
	}

	minimalConfig := goreleaserConfig.Project{
		ProjectName: config.ProjectName,
		Dist:        config.Dist,
		GoMod:       config.GoMod,
		Env:         config.Env,
		Builds:      builds,
		Dockers:     dockers,
	}
	bytes, err := yaml.Marshal(minimalConfig)
	if err != nil {
		return err
	}
	return os.WriteFile(".goreleaser-minimal.yml", bytes, 0o644)
}
