//go:build tools
// +build tools

package tools

// go packages containing .proto files we depend on.
import (
	_ "github.com/gogo/protobuf"
	_ "github.com/grpc-ecosystem/grpc-gateway"
	_ "k8s.io/api"
	_ "k8s.io/apimachinery"
)
