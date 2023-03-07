package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/pkg/errors"
)

// Quick Build
func LocalDev() {
	mg.Deps(BootstrapTools)
	mg.Deps(BootstrapProto)
	mg.Deps(mg.F(BuildDockers, "bundle"))
	mg.Deps(mg.F(BuildDockers, "ask"))
	mg.Deps(Kind)
	mg.Deps(StartDependencies)
}

// install go tools
func BootstrapTools() error {
	mg.Deps(goCheck)
	packages, err := goOutput("list", "-f", "{{range .Imports}}{{.}} {{end}}", "internal/tools/tools.go")
	if err != nil {
		return err
	}
	for _, p := range strings.Split(strings.TrimSpace(packages), " ") {
		err := goRun("install", p)
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

// Build the lookout UI from internal/lookout/ui
func BuildLookoutUI(arg string) error {
	// if arg is "ask" then ask the user if they want to build the UI
	if arg == "ask" {
		// Scan for Y press
		var input string
		fmt.Print("Do you want to build the Lookout UI? [Y/n] ")
		fmt.Scanln(&input)
		if input != "Y" {
			return nil
		}
	}

	// Only build the UI if the yarn binary is present
	if err := yarnCheck(); err != nil {
		return nil
	}

	mg.Deps(yarnInstall)
	mg.Deps(yarnOpenAPI)
	mg.Deps(yarnBuild)
	return nil
}

// run integration test
func CiIntegrationTests() {
	mg.Deps(BootstrapTools)
	mg.Deps(mg.F(goreleaserMinimalRelease, "bundle"), Kind)
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
