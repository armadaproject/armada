package main

import (
	"github.com/magefile/mage/sh"
)

func goreleaserBinary() string {
	return binaryWithExt("goreleaser")
}

func goreleaserRun(args ...string) error {
	return sh.Run(goreleaserBinary(), args...)
}
