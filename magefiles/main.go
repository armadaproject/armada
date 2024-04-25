package main

import (
	"fmt"
	"os"
	"path/filepath"
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

	requiredTools := &ToolsList{}
	if err := readYaml("tools.yaml", requiredTools); err != nil {
		return err
	}

	installedToolsFilePath, err := getArmadaToolsFilePath()
	if err != nil {
		return err
	}
	installedTools := &ToolsList{}
	if err := readYaml(installedToolsFilePath, installedTools); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Create a map of existing tools for quick lookup
	installedToolsMap := make(map[string]bool)
	for _, installedTool := range installedTools.Tools {
		installedToolsMap[installedTool] = true
	}

	if len(requiredTools.Tools) > 0 {
		var envPaths map[string]string
		if isGitHubActions() {
			envPaths = make(map[string]string)
			for _, cache := range []string{"GOMODCACHE", "GOCACHE"} {
				path, err := os.MkdirTemp("", cache)
				if err != nil {
					return fmt.Errorf("error creating temporary %s directory: %w", cache, err)
				}
				envPaths[cache] = path
			}

			defer func() {
				if err := goRunWith(envPaths, "clean", "-cache", "-modcache"); err != nil {
					fmt.Printf("Error occurred while running 'go clean': %v\n", err)
				}
			}()
		}

		for _, requiredTool := range requiredTools.Tools {
			if !installedToolsMap[requiredTool] {
				if err := goRunWith(envPaths, "install", requiredTool); err != nil {
					return err
				}
				installedTools.Tools = append(installedTools.Tools, requiredTool)
				if err := writeYAML(installedToolsFilePath, installedTools); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func isGitHubActions() bool {
	return strings.ToLower(os.Getenv("GITHUB_ACTIONS")) == "true"
}

func getArmadaToolsFilePath() (string, error) {
	goBinDir, err := goEnv("GOBIN")
	if err != nil {
		return "", err
	}
	if goBinDir == "" {
		goPathDir, err := goEnv("GOPATH")
		if err != nil {
			return "", err
		}
		goBinDir = filepath.Join(goPathDir, "bin")
	}
	return filepath.Join(goBinDir, ".armada-tools.yaml"), nil
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
	mg.Deps(BootstrapTools)
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
		mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind, downloadDependencyImages)
	case "full":
		mg.Deps(BuildPython, mg.F(BuildDockers, "bundle, lookout-bundle, jobservice"), Kind, downloadDependencyImages)
	case "no-build", "debug":
		mg.Deps(Kind, downloadDependencyImages)
	default:
		return fmt.Errorf("invalid localdev mode: %s; valid modes are: minimal, full, no-build, debug", arg)
	}

	mg.Deps(StartDependencies)
	fmt.Println("Waiting for dependencies to start...")
	mg.Deps(CheckForPulsarRunning)

	switch arg {
	case "minimal":
		os.Setenv("ARMADA_COMPONENTS", "executor,server,scheduler")
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

// Helper function to write YAML data to a file
func writeYAML(filename string, data interface{}) error {
	bytes, err := yaml.Marshal(data)
	if err != nil {
		return err
	}
	err = os.WriteFile(filename, bytes, 0o644)
	if err != nil {
		return err
	}
	return nil
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

func Generate() error {
	return goRun("generate", "./...")
}

// CI Image to build
func BuildCI() error {
	ciImage := []string{"bundle", "lookout-bundle", "server", "executor", "armadactl", "testsuite", "lookoutv2", "lookoutingesterv2", "eventingester", "scheduler", "scheduleringester", "binoculars", "jobservice"}
	err := goreleaserMinimalRelease(ciImage...)
	if err != nil {
		return err
	}
	return nil
}
