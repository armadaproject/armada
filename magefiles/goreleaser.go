package main

import (
	"fmt"
	"os"
	"time"

	goreleaserConfig "github.com/goreleaser/goreleaser/pkg/config"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v2"
)

const (
	GORELEASER_CONFIG_PATH         = "./.goreleaser.yml"
	GORELEASER_MINIMAL_CONFIG_PATH = "./.goreleaser-minimal.yml"
)

func goreleaserBinary() string {
	return binaryWithExt("goreleaser")
}

func goreleaserRun(args ...string) error {
	return sh.Run(goreleaserBinary(), args...)
}

func goreleaserMinimalRelease(dockerIds ...string) error {
	if err := goreleaserWriteMinimalReleaseConfig(dockerIds...); err != nil {
		return err
	}

	timeTaken := time.Now()
	err := goreleaserRun("release", "--snapshot", "--rm-dist", "-f", GORELEASER_MINIMAL_CONFIG_PATH)
	fmt.Println("Time to build dockers:", time.Since(timeTaken))

	return err
}

// Write a minimal goreleaser config containing only the subset of targets
// necessary for building the specified Docker images.
func goreleaserWriteMinimalReleaseConfig(dockerIds ...string) error {
	if len(dockerIds) == 0 {
		return nil
	}
	config, err := goreleaserConfig.Load(GORELEASER_CONFIG_PATH)
	if err != nil {
		return err
	}

	dockerIdsToBuild := set(dockerIds)
	dockersById := make(map[string]goreleaserConfig.Docker)
	buildIds := make(map[string]bool)
	for _, docker := range config.Dockers {
		if dockerIdsToBuild[docker.ID] {
			dockersById[docker.ID] = docker
			for _, id := range docker.IDs {
				buildIds[id] = true
			}
		}
	}
	for dockerId := range dockerIdsToBuild {
		if _, ok := dockersById[dockerId]; !ok {
			return errors.Errorf("docker id %s not found in %s", dockerId, GORELEASER_CONFIG_PATH)
		}
	}

	builds := make([]goreleaserConfig.Build, 0)
	for _, build := range config.Builds {
		if buildIds[build.ID] {
			builds = append(builds, build)
		}
	}
	if len(builds) == 0 {
		return errors.Errorf("%v matched no builds in %s", buildIds, GORELEASER_CONFIG_PATH)
	}

	targets := make(map[string]bool)
	for _, docker := range dockersById {
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
		Dockers:     maps.Values(dockersById),
	}
	bytes, err := yaml.Marshal(minimalConfig)
	if err != nil {
		return err
	}
	return os.WriteFile(GORELEASER_MINIMAL_CONFIG_PATH, bytes, 0o644)
}
