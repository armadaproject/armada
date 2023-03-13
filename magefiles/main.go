package main

import (
	"fmt"
	"os"
	"sigs.k8s.io/yaml"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/pkg/errors"
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
	mg.Deps(kindCheck)
	mg.Deps(kindSetup)
	mg.Deps(kindWaitUntilReady)
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

func BuildCICluster() {
	mg.Deps(BootstrapTools)
	mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind)
	mg.Deps(ciSetup)
}

// run integration test
func CiIntegrationTests() {
	mg.Deps(BuildCICluster)
	mg.Deps(ciRunTests)
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

// readYaml reads a yaml file and unmarshalls the result into out
func readYaml(filename string, out interface{}) error {
	bytes, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(bytes, out)
	return err
}
