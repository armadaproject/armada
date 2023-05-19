ineffassign ./...
go install golang.org/x/tools/cmd/goimports@v0.1.1
goimports -l -local "github.com/armadaproject/armada" ./internal
gofumpt -l ./internal
