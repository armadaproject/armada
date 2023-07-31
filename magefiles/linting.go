package main

import (
	"fmt"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const GOLANGCI_LINT_VERSION_CONSTRAINT = ">= 1.52.0"

// Extract the version of golangci-lint
func golangciLintVersion() (*semver.Version, error) {
	output, err := golangcilintOutput("--version")
	if err != nil {
		return nil, errors.Errorf("error running version cmd: %v", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) < 4 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(strings.TrimPrefix(fields[3], "v")) // adjusted index and removed prefix 'v'
	if err != nil {
		return nil, errors.Errorf("error parsing version: %v", err)
	}
	return version, nil
}

// Check if the version of golangci-lint meets the predefined constraints
func golangciLintCheck() error {
	version, err := golangciLintVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v", err)
	}
	constraint, err := semver.NewConstraint(GOLANGCI_LINT_VERSION_CONSTRAINT)
	if err != nil {
		return errors.Errorf("error parsing constraint: %v", err)
	}
	if !constraint.Check(version) {
		return errors.Errorf("found version %v but it failed constraint %v", version, constraint)
	}
	return nil
}

// Fixing Linting
func LintFix() error {
	mg.Deps(golangciLintCheck)
	cmd, err := go_TEST_CMD()
	if err != nil {
		return err
	}
	if len(cmd) == 0 {
		output, err := golangcilintOutput("run", "--fix", "--timeout", "10m")
		if err != nil {
			fmt.Printf("error fixing linting cmd: %v", err)
			fmt.Printf("\nOutput: %s\n", output)
		}
	} else {
		cmd = append(cmd, "golangci-lint", "run", "--fix", "--timeout", "10m")
		output, err := dockerOutput(cmd...)
		fmt.Println(output)
		if err != nil {
			return err
		}
	}
	return nil
}

// Linting Check
func CheckLint() error {
	mg.Deps(golangciLintCheck)
	cmd, err := go_TEST_CMD()
	if err != nil {
		return err
	}
	if len(cmd) == 0 {
		output, err := golangcilintOutput("run", "--timeout", "10m")
		if err != nil {
			fmt.Printf("error fixing linting cmd: %v", err)
			fmt.Printf("\nOutput: %s\n", output)
		}
	} else {
		cmd = append(cmd, "golangci-lint", "run", "--timeout", "10m")
		output, err := dockerOutput(cmd...)
		fmt.Println(output)
		if err != nil {
			return err
		}
	}
	return nil
}

func golangcilintBinary() string {
	return binaryWithExt("golangci-lint")
}

func golangcilintOutput(args ...string) (string, error) {
	return sh.Output(golangcilintBinary(), args...)
}

// func golangcilintRun(args ...string) error {
// 	return sh.Run(golangcilintBinary(), args...)
// }
