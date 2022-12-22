package main

import (
	"github.com/magefile/mage/sh"
)

func goreleaserBinary() string {
	return binaryWithExt("goreleaser")
}

func goreleaserOutput(args ...string) (string, error) {
	return sh.Output(goreleaserBinary(), args...)
}

func goreleaserRun(args ...string) error {
	return sh.Run(goreleaserBinary(), args...)
}
