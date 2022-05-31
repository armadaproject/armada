//go:build tools
// +build tools

package tools

// These are dependencies for proto generation.
// Placing them in this file allows us to get the dependencies via go get
// but they installed.
import (
	_ "github.com/gogo/protobuf/protoc-gen-gogofaster"
	_ "k8s.io/api"
	_ "k8s.io/apimachinery"
)
