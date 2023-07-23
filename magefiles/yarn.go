package main

import (
	"os"

	"github.com/magefile/mage/sh"
)

// Create golang code to build the UI
func yarnBinary() string {
	return binaryWithExt("yarn")
}

func yarnRun(args ...string) error {
	if err := os.Chdir("internal/lookout/ui"); err != nil {
		return err
	}

	if err := sh.RunV(yarnBinary(), args...); err != nil {
		return err
	}

	if err := os.Chdir("../../.."); err != nil {
		return err
	}

	return nil
}

func yarnInstall() error {
	return yarnRun("install")
}

func yarnOpenAPI() error {
	if checkOs() == "windows" {
		return yarnRun("run", "openapi:win")
	}
	return yarnRun("run", "openapi")
}

func yarnBuild() error {
	return yarnRun("build")
}

func yarnCheck() error {
	return sh.Run(yarnBinary(), "--version")
}
