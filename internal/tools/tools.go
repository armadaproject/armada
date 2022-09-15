//go:build tools
// +build tools

package tools

import (
	_ "github.com/go-swagger/go-swagger/cmd/swagger"
	_ "github.com/gordonklaus/ineffassign"
	_ "github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway"
	_ "github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger"
	_ "github.com/jstemmer/go-junit-report"
	_ "github.com/kyleconroy/sqlc/cmd/sqlc"
	_ "github.com/mitchellh/gox"
	_ "github.com/wlbr/templify"
	_ "golang.org/x/tools/cmd/goimports"
	_ "sigs.k8s.io/kind"
)
