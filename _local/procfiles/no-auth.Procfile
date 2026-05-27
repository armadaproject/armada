server: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-server ./cmd/server/main.go && ./dist/armada-server --config ./_local/server/config.yaml
scheduler: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-scheduler ./cmd/scheduler/main.go && ./dist/armada-scheduler run --config ./_local/scheduler/config.yaml
scheduleringester: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-scheduleringester ./cmd/scheduleringester/main.go && ./dist/armada-scheduleringester --config ./_local/scheduleringester/config.yaml
eventingester: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-eventingester ./cmd/eventingester/main.go && ./dist/armada-eventingester --config ./_local/eventingester/config.yaml
executor: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-executor ./cmd/executor/main.go && ./dist/armada-executor --config ./_local/executor/config.yaml
lookout: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-lookout ./cmd/lookout/main.go && ./dist/armada-lookout --config ./_local/lookout/config.yaml
lookoutingester: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-lookoutingester ./cmd/lookoutingester/main.go && ./dist/armada-lookoutingester --config ./_local/lookoutingester/config.yaml
binoculars: ${GO_BIN:-go} build -gcflags="all=-N -l" -o ./dist/armada-binoculars ./cmd/binoculars/main.go && ./dist/armada-binoculars --config ./_local/binoculars/config.yaml
lookoutui: sh -c 'cd internal/lookoutui && yarn install && yarn run openapi && PROXY_TARGET=http://localhost:8089 yarn dev'
