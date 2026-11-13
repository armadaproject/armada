package main

import (
	"os/exec"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/pkg/errors"
)

// BuildRust builds the armada Rust client, regenerating its pre-generated proto files in client/rust/src/gen.
func BuildRust() error {
	mg.Deps(BootstrapProto)
	if _, err := exec.LookPath("cargo"); err != nil {
		return errors.Errorf("cargo not found; install the Rust toolchain to build the Rust client")
	}
	return sh.RunWith(
		map[string]string{"ARMADA_GENERATE": "1"},
		"cargo", "build", "--manifest-path", "client/rust/Cargo.toml",
	)
}
