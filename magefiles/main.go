package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"
)

// BootstrapTools installs all tools needed tobuild and release armada
// For the list of tools this will install, see tools.yaml in the root directory
func BootstrapTools() error {
	mg.Deps(goCheck)
	type ToolsList struct {
		Tools []string
	}

	tools := &ToolsList{}
	err := readYaml("tools.yaml", tools)
	if err != nil {
		return err
	}

	for _, tool := range tools.Tools {
		err := goRun("install", tool)
		if err != nil {
			return err
		}
	}
	return nil
}

// check dependent tools are present and the correct version
func CheckDeps() error {
	checks := []struct {
		name  string
		check func() error
	}{
		{"docker", dockerCheck},
		{"docker-compose", dockerComposeCheck},
		{"go", goCheck},
		{"kind", kindCheck},
		{"kubectl", kubectlCheck},
		{"protoc", protocCheck},
		{"sqlc", sqlcCheck},
	}
	failures := false
	for _, check := range checks {
		fmt.Printf("Checking %s... ", check.name)
		if err := check.check(); err != nil {
			fmt.Printf("FAILED\nReason: %v\n", err)
			failures = true
		} else {
			fmt.Println("PASSED")
		}
	}
	if failures {
		return errors.New("check(s) failed.")
	}
	return nil
}

// cleans proto files
func Clean() {
	fmt.Println("Cleaning...")
	for _, path := range []string{"proto", "protoc", ".goreleaser-minimal.yml", "dist", ".kube"} {
		os.RemoveAll(path)
	}
}

// setup kind and wait for it to be ready
func Kind() {
	timeTaken := time.Now()
	mg.Deps(kindCheck)
	mg.Deps(kindSetup)
	mg.Deps(kindWaitUntilReady)
	fmt.Println("Time to setup kind:", time.Since(timeTaken))
}

// teardown kind
func KindTeardown() {
	mg.Deps(kindCheck)
	mg.Deps(kindTeardown)
}

// generate scheduler sql
func Sql() error {
	mg.Deps(sqlcCheck)
	return sqlcRun("generate", "-f", "internal/scheduler/database/sql.yaml")
}

// generate protos
func Proto() {
	mg.Deps(BootstrapProto)
	mg.Deps(protoGenerate)
}

func BootstrapProto() {
	mg.Deps(protocCheck)
	mg.Deps(protoInstallProtocArmadaPlugin, protoPrepareThirdPartyProtos)
}

func BuildDockers(arg string) error {
	dockerIds := make([]string, 0)
	for _, s := range strings.Split(arg, ",") {
		dockerIds = append(dockerIds, strings.TrimSpace(s))
	}
	if err := goreleaserMinimalRelease(dockerIds...); err != nil {
		return err
	}
	return nil
}

// Create a Local Armada Cluster
func LocalDev(arg string) error {
	timeTaken := time.Now()
	mg.Deps(BootstrapTools)
	fmt.Println("Time to bootstrap tools:", time.Since(timeTaken))

	validArgs := []string{"minimal", "full", "no-build"}

	if !strings.Contains(strings.Join(validArgs, ","), arg) {
		return errors.Errorf("invalid argument: %s", arg)
	}

	switch arg {
	case "minimal":
		timeTaken := time.Now()
		mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind, downloadDependencyImages)
		fmt.Printf("Time to build, setup kind and download images: %s\n", time.Since(timeTaken))
	case "full":
		mg.Deps(mg.F(BuildDockers, "bundle, lookout-bundle, jobservice"), Kind, downloadDependencyImages)
	case "no-build":
		mg.Deps(Kind, downloadDependencyImages)
	}

	mg.Deps(StartDependencies)
	fmt.Println("Waiting for dependencies to start...")
	mg.Deps(CheckForPulsarRunning)

	if arg == "minimal" {
		err := dockerComposeRun("up", "-d", "executor")
		if err != nil {
			return err
		}
		err = dockerComposeRun("up", "-d", "server")
		if err != nil {
			return err
		}
	} else {
		mg.Deps(StartComponents)
	}

	fmt.Println("Run: `docker-compose logs -f` to see logs")
	return nil
}

// Stop Local Armada Cluster
func LocalDevStop() {
	mg.Deps(StopComponents)
	mg.Deps(StopDependencies)
	mg.Deps(KindTeardown)
}

// Build the lookout UI from internal/lookout/ui
func UI() error {
	mg.Deps(yarnCheck)

	mg.Deps(yarnInstall)
	mg.Deps(yarnOpenAPI)
	mg.Deps(yarnBuild)
	return nil
}

// readYaml reads a yaml file and unmarshalls the result into out
func readYaml(filename string, out interface{}) error {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(bytes, out)
	return err
}
