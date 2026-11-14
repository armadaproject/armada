package main

import (
	"fmt"
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

const golangciLintModule = "github.com/golangci/golangci-lint/v2/cmd/golangci-lint"

const golangciLintInstallHint = "run `mage BootstrapTools` and put Go's tool folder ($(go env GOPATH)/bin, or GOBIN if set) first on your PATH"

// Extract the golangci-lint version pinned in tools.yaml. The Go lint workflow pins the same version separately.
func golangciLintPinnedVersion() (*semver.Version, error) {
	tools := &struct{ Tools []string }{}
	if err := readYaml("tools.yaml", tools); err != nil {
		return nil, err
	}
	for _, tool := range tools.Tools {
		if version, found := strings.CutPrefix(tool, golangciLintModule+"@"); found {
			return semver.NewVersion(version)
		}
	}
	return nil, errors.Errorf("%s not found in tools.yaml", golangciLintModule)
}

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

// Check that golangci-lint is the version pinned in tools.yaml
func golangciLintCheck() error {
	pinned, err := golangciLintPinnedVersion()
	if err != nil {
		return errors.Errorf("error getting pinned version: %v", err)
	}
	version, err := golangciLintVersion()
	if err != nil {
		return errors.Errorf("error getting version: %v; to install golangci-lint %v, %s", err, pinned, golangciLintInstallHint)
	}
	if !version.Equal(pinned) {
		return errors.Errorf("found golangci-lint version %v but tools.yaml pins %v; to install it, %s", version, pinned, golangciLintInstallHint)
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
			return err
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
func LintCheck() error {
	mg.Deps(golangciLintCheck)
	cmd, err := go_TEST_CMD()
	if err != nil {
		return err
	}
	if len(cmd) == 0 {
		output, err := golangcilintOutput("run", "--timeout", "10m")
		if err != nil {
			fmt.Printf("error running linting cmd: %v", err)
			fmt.Printf("\nOutput: %s\n", output)
			return err
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
