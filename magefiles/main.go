package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
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

// Download install the bootstap tools and download mod and make it tidy
func Download() error {
	mg.Deps(BootstrapTools)
	go_test_cmd, err := go_TEST_CMD()
	if err != nil {
		return err
	}
	if len(go_test_cmd) == 0 {
		if err = sh.Run("go", "mod", "download"); err != nil {
			return err
		}
		if err = sh.Run("go", "mod", "tidy"); err != nil {
			return err
		}
	} else {
		cmd := append(go_test_cmd, "go", "mod", "download")
		if err := dockerRun(cmd...); err != nil {
			return err
		}
		cmd = append(go_test_cmd, "go", "mod", "tidy")
		if err := dockerRun(cmd...); err != nil {
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
		{"docker compose", dockerComposeCheck},
		{"docker buildx", dockerBuildxCheck},
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

// Generate Helm documentation.
func HelmDocs() error {
	fmt.Println("Generating Helm documentation...")
	output, err := sh.Output("./scripts/helm-docs.sh")
	if err != nil {
		fmt.Println(output)
		return fmt.Errorf("failed to generate Helm documentation: %w", err)
	} else {
		fmt.Println(output)
	}
	return nil
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

	// Set the Executor Update Frequency to 1 second for local development
	os.Setenv("ARMADA_SCHEDULING_EXECUTORUPDATEFREQUENCY", "1s")

	switch arg {
	case "minimal":
		timeTaken := time.Now()
		os.Setenv("PULSAR_BACKED", "")
		mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind, downloadDependencyImages)
		fmt.Printf("Time to build, setup kind and download images: %s\n", time.Since(timeTaken))
	case "minimal-pulsar":
		mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind, downloadDependencyImages)
	case "full":
		mg.Deps(BuildPython, mg.F(BuildDockers, "bundle, lookout-bundle, jobservice"), Kind, downloadDependencyImages)
	case "no-build", "debug":
		mg.Deps(Kind, downloadDependencyImages)
	default:
		return errors.Errorf("invalid argument for Localdev: %s", arg)
	}

	mg.Deps(StartDependencies)
	fmt.Println("Waiting for dependencies to start...")
	mg.Deps(CheckForPulsarRunning)

	switch arg {
	case "minimal":
		os.Setenv("ARMADA_COMPONENTS", "executor,server")
		mg.Deps(StartComponents)
	case "minimal-pulsar":
		// This 20s sleep is to remedy an issue caused by pods coming up too fast after pulsar
		// TODO: Deal with this internally somehow?
		os.Setenv("ARMADA_COMPONENTS", "executor-pulsar,server-pulsar,scheduler,scheduleringester")
		mg.Deps(StartComponents)
	case "debug", "no-build":
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

// junitReport Output test results in Junit format, e.g., to display in Jenkins.
func JunitReport() error {
	if err := os.MkdirAll("test_reports", os.ModePerm); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Make sure everything has been synced to disk
	if err := sh.RunV("sync"); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	// Remove junit.xml file if it exists
	if err := os.Remove("test_reports/junit.xml"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file: %v", err)
	}

	// Get the command for the go test
	goTestCmd, err := go_TEST_CMD()
	if err != nil {
		return err
	}

	if len(goTestCmd) == 0 {
		if err := sh.RunV("bash", "-c", "cat test_reports/*.txt | go-junit-report > test_reports/junit.xml"); err != nil {
			return err
		}
	} else {
		goTestCmd = append(goTestCmd, "bash", "-c", "cat test_reports/*.txt | go-junit-report > test_reports/junit.xml")
		if err = dockerRun(goTestCmd...); err != nil {
			return err
		}
	}
	return nil
}

// Code generation tasks: statik, goimports, go generate.
func Generate() error {
	go_cmd, err := go_CMD()
	if err != nil {
		return err
	}

	// Commands to be run
	cmd1 := []string{
		"go", "run", "github.com/rakyll/statik",
		"-dest=internal/lookout/repository/schema/",
		"-src=internal/lookout/repository/schema/",
		"-include=\\*.sql",
		"-ns=lookout/sql",
		"-Z",
		"-f",
		"-m",
	}
	cmd2 := []string{
		"go", "run", "golang.org/x/tools/cmd/goimports",
		"-w",
		"-local", "github.com/armadaproject/armada",
		"internal/lookout/repository/schema/statik",
	}

	if len(go_cmd) == 0 {
		if err = goRun(cmd1[1:]...); err != nil {
			return err
		}
		if err = goRun(cmd2[2:]...); err != nil {
			return err
		}
	} else {
		dockercmd := append(go_cmd, cmd1...)
		dockercmd = append(dockercmd, "&&")
		dockercmd = append(dockercmd, cmd2...)
		fmt.Println(dockercmd)
		if err := dockerRun(go_cmd...); err != nil {
			return err
		}
	}
	if err = goRun("generate", "./..."); err != nil {
		return err
	}
	return nil
}

// CI Image to build
func BuildCI() error {
	ciImage := []string{"bundle", "lookout-bundle", "server", "executor", "armadactl", "testsuite", "lookout", "lookoutingester", "lookoutv2", "lookoutingesterv2", "eventingester", "scheduler", "scheduleringester", "binoculars", "jobservice"}
	err := goreleaserMinimalRelease(ciImage...)
	if err != nil {
		return err
	}
	return nil
}
