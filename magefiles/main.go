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

// BootstrapTools installs all tools needed tobuild and release Armada.
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

// Check dependent tools are present and the correct version.
func CheckDeps() error {
	checks := []struct {
		name  string
		check func() error
	}{
		{"docker", dockerCheck},
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

// Cleans proto files.
func Clean() {
	fmt.Println("Cleaning...")
	for _, path := range []string{"proto", "protoc", ".goreleaser-minimal.yml", "dist", ".kube"} {
		os.RemoveAll(path)
	}
}

// Setup Kind and wait for it to be ready
func Kind() {
	timeTaken := time.Now()
	mg.Deps(kindCheck)
	mg.Deps(kindSetup)
	mg.Deps(kindWaitUntilReady)
	fmt.Println("Time to setup kind:", time.Since(timeTaken))
}

// Teardown Kind Cluster
func KindTeardown() {
	mg.Deps(kindCheck)
	mg.Deps(kindTeardown)
}

// Generate scheduler SQL.
func Sql() error {
	mg.Deps(sqlcCheck)
	return sqlcRun("generate", "-f", "internal/scheduler/database/sql.yaml")
}

// Generate Protos.
func Proto() {
	mg.Deps(BootstrapProto)
	mg.Deps(protoGenerate)
}

// Ensures the Protobuf dependencies are installed.
func BootstrapProto() {
	mg.Deps(protocCheck)
	mg.Deps(protoInstallProtocArmadaPlugin, protoPrepareThirdPartyProtos)
}

// Builds the specified docker images.
func BuildDockers(arg string) error {
	dockerIds := make([]string, 0)
	timeTaken := time.Now()
	for _, s := range strings.Split(arg, ",") {
		dockerIds = append(dockerIds, strings.TrimSpace(s))
	}
	if err := goreleaserMinimalRelease(dockerIds...); err != nil {
		return err
	}
	fmt.Println("Time to build dockers:", time.Since(timeTaken))
	return nil
}

// Create a Local Armada Cluster.
func LocalDev(arg string) error {
	timeTaken := time.Now()
	mg.Deps(BootstrapTools)
	fmt.Println("Time to bootstrap tools:", time.Since(timeTaken))

	switch arg {
	case "minimal":
		timeTaken := time.Now()
		mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind, downloadDependencyImages)
		fmt.Printf("Time to build, setup kind and download images: %s\n", time.Since(timeTaken))
	case "full":
		mg.Deps(mg.F(BuildDockers, "bundle, lookout-bundle, jobservice"), Kind, downloadDependencyImages)
	case "no-build", "debug":
		mg.Deps(Kind, downloadDependencyImages)
	default:
		return errors.Errorf("invalid argument: %s", arg)
	}

	mg.Deps(StartDependencies)
	fmt.Println("Waiting for dependencies to start...")
	mg.Deps(CheckForPulsarRunning)

	switch arg {
	case "minimal":
		os.Setenv("ARMADA_COMPONENTS", "executor,server")
		mg.Deps(StartComponents)
	case "debug":
		fmt.Println("Dependencies started, ending localdev...")
		return nil
	default:
		mg.Deps(StartComponents)
	}

	fmt.Println("Run: `docker compose logs -f` to see logs")
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
	timeTaken := time.Now()
	mg.Deps(yarnCheck)

	mg.Deps(yarnInstall)
	mg.Deps(yarnOpenAPI)
	mg.Deps(yarnBuild)

	fmt.Println("Time to build UI:", time.Since(timeTaken))
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
