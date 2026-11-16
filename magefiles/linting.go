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
	tools := &ToolsList{}
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
		return nil, errors.Wrap(err, "error running version cmd")
	}
	fields := strings.Fields(string(output))
	if len(fields) < 4 {
		return nil, errors.Errorf("unexpected version cmd output: %s", output)
	}
	version, err := semver.NewVersion(strings.TrimPrefix(fields[3], "v")) // adjusted index and removed prefix 'v'
	if err != nil {
		return nil, errors.Wrap(err, "error parsing version")
	}
	return version, nil
}

// Check that native golangci-lint is at least the version pinned in tools.yaml
func golangciLintCheck() error {
	pinned, err := golangciLintPinnedVersion()
	if err != nil {
		return errors.Wrap(err, "error getting pinned version")
	}
	version, err := golangciLintVersion()
	if err != nil {
		return errors.Wrapf(err, "error getting version; to install golangci-lint %v, %s", pinned, golangciLintInstallHint)
	}
	if err := constraintCheck(version, ">= "+pinned.String(), "golangci-lint"); err != nil {
		return errors.Wrapf(err, "to install golangci-lint %v, %s", pinned, golangciLintInstallHint)
	}
	return nil
}

// Fixing Linting
func LintFix() error {
	return runGolangciLint("--fix")
}

// Linting Check
func LintCheck() error {
	return runGolangciLint()
}

func runGolangciLint(args ...string) error {
	cmd, err := go_TEST_CMD()
	if err != nil {
		return err
	}
	lintArgs := append([]string{"run"}, args...)
	lintArgs = append(lintArgs, "--timeout", "10m")
	if len(cmd) == 0 {
		mg.Deps(golangciLintCheck)
		output, err := golangcilintOutput(lintArgs...)
		if err != nil {
			fmt.Println(output)
		}
		return err
	}
	cmd = append(cmd, "golangci-lint")
	output, err := dockerOutput(append(cmd, lintArgs...)...)
	fmt.Println(output)
	return err
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
