server: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-server ./cmd/server/main.go && dlv dap --listen=:2345
scheduler: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-scheduler ./cmd/scheduler/main.go && dlv dap --listen=:2346
scheduleringester: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-scheduleringester ./cmd/scheduleringester/main.go && dlv dap --listen=:2347
eventingester: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-eventingester ./cmd/eventingester/main.go && dlv dap --listen=:2348
executor: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-executor ./cmd/executor/main.go && dlv dap --listen=:2349
lookout: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-lookout ./cmd/lookout/main.go && dlv dap --listen=:2350
lookoutingester: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-lookoutingester ./cmd/lookoutingester/main.go && dlv dap --listen=:2351
binoculars: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-binoculars ./cmd/binoculars/main.go && dlv dap --listen=:2352
lookoutui: sh -c 'cd internal/lookoutui && yarn install && yarn run openapi && PROXY_TARGET=http://localhost:18089 yarn dev'
