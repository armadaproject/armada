# Determine which platform we're on based on the kernel name
platform := $(shell uname -s || echo unknown)

# Check that all necessary executables are present
# Using 'where' on Windows and 'which' on Unix-like systems, respectively
# We do not check for 'date', since it's a cmdlet on Windows, which do not show up with where
# (:= assignment is necessary to force immediate evaluation of expression)
EXECUTABLES = go git docker
ifeq ($(platform),windows32)
	K := $(foreach exec,$(EXECUTABLES),$(if $(shell where $(exec)),some string,$(error "No $(exec) in PATH")))
else
	K := $(foreach exec,$(EXECUTABLES),$(if $(shell which $(exec)),some string,$(error "No $(exec) in PATH")))
endif

# Get the current date and time (to insert into go build)
# On Windows, we need to use the powershell date command (alias of Get-Date) to get the full date-time string
ifeq ($(platform),unknown)
	date := $(unknown)
else ifeq ($(platform),windows32)
	date := $(shell powershell -c date || unknown)
else
	date := $(shell date || unknown)
endif
BUILD_TIME = $(strip $(date)) # Strip leading/trailing whitespace (added by powershell)

# Get go version
# (using subst to change, e.g., 'go version go1.17.2 windows/amd64' to 'go1.17.2 windows/amd64')
GO_VERSION = $(strip $(subst go version,,$(shell go version)))

# Get most recent git commit (to insert into go build)
GIT_COMMIT := $(shell git rev-list --abbrev-commit -1 HEAD)

# The RELEASE_VERSION environment variable is set by circleci (to insert into go build and output filenames)
ifndef RELEASE_VERSION
override RELEASE_VERSION = UNKNOWN_VERSION
endif

# use bash for running:
export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit
export KUBECONFIG:=e2e/setup/config

gobuildlinux = GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"
gobuild = go build

build-server:
	$(gobuild) -o ./bin/server cmd/armada/main.go

build-executor:
	$(gobuild) -o ./bin/executor cmd/executor/main.go

build-fakeexecutor:
	$(gobuild) -o ./bin/executor cmd/fakeexecutor/main.go

ARMADACTL_BUILD_PACKAGE := github.com/G-Research/armada/internal/armadactl/build
define ARMADACTL_LDFLAGS
-X '$(ARMADACTL_BUILD_PACKAGE).BuildTime=$(BUILD_TIME)' \
-X '$(ARMADACTL_BUILD_PACKAGE).ReleaseVersion=$(RELEASE_VERSION)' \
-X '$(ARMADACTL_BUILD_PACKAGE).GitCommit=$(GIT_COMMIT)' \
-X '$(ARMADACTL_BUILD_PACKAGE).GoVersion=$(GO_VERSION)'
endef
build-armadactl:
	$(gobuild) -ldflags="$(ARMADACTL_LDFLAGS)" -o ./bin/armadactl cmd/armadactl/main.go

build-armadactl-multiplatform:
	go install github.com/mitchellh/gox@v1.0.1
	gox -ldflags="$(ARMADACTL_LDFLAGS)" -output="./bin/{{.OS}}-{{.Arch}}/armadactl" -arch="amd64" -os="windows linux darwin" ./cmd/armadactl/

build-armadactl-release: build-armadactl-multiplatform
	mkdir ./dist || true
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-linux-amd64.tar.gz -C ./bin/linux-amd64/ armadactl
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-darwin-amd64.tar.gz -C ./bin/darwin-amd64/ armadactl
	zip -j ./dist/armadactl-$(RELEASE_VERSION)-windows-amd64.zip ./bin/windows-amd64/armadactl.exe

build-binoculars:
	$(gobuild) -o ./bin/binoculars cmd/binoculars/main.go	

build-load-tester:
	$(gobuild) -o ./bin/armada-load-tester cmd/armada-load-tester/main.go

build: build-server build-executor build-fakeexecutor build-armadactl build-load-tester build-binoculars

build-docker-server:
	$(gobuildlinux) -o ./bin/linux/server cmd/armada/main.go
	docker build $(dockerFlags) -t armada -f ./build/armada/Dockerfile .

build-docker-executor:
	$(gobuildlinux) -o ./bin/linux/executor cmd/executor/main.go
	docker build $(dockerFlags) -t armada-executor -f ./build/executor/Dockerfile .

build-docker-armada-load-tester:
	$(gobuildlinux) -o ./bin/linux/armada-load-tester cmd/armada-load-tester/main.go
	docker build $(dockerFlags) -t armada-load-tester -f ./build/armada-load-tester/Dockerfile .

build-docker-armadactl:
	$(gobuildlinux) -o ./bin/linux/armadactl cmd/armadactl/main.go
	docker build $(dockerFlags) -t armadactl -f ./build/armadactl/Dockerfile .

build-docker-fakeexecutor:
	$(gobuildlinux) -o ./bin/linux/fakeexecutor cmd/fakeexecutor/main.go
	docker build $(dockerFlags) -t armada-fakeexecutor -f ./build/fakeexecutor/Dockerfile .

build-docker-lookout:
	(cd ./internal/lookout/ui/ && npm ci && npm run openapi && npm run build)
	$(gobuildlinux) -o ./bin/linux/lookout cmd/lookout/main.go
	docker build $(dockerFlags) -t armada-lookout -f ./build/lookout/Dockerfile .

build-docker-binoculars:
	$(gobuildlinux) -o ./bin/linux/binoculars cmd/binoculars/main.go
	docker build $(dockerFlags) -t armada-binoculars -f ./build/binoculars/Dockerfile .

build-docker: build-docker-server build-docker-executor build-docker-armadactl build-docker-armada-load-tester build-docker-fakeexecutor build-docker-lookout build-docker-binoculars

build-docker-e2e-tests: build-ci
	docker build $(dockerFlags) -t tests/armada-server:latest -f ./build/armada/Dockerfile .
	docker build $(dockerFlags) -t tests/armada-executor:latest -f ./build/executor/Dockerfile .

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-armadactl-multiplatform build-load-tester

.ONESHELL:
tests:
	docker run -d --name=test-redis -p=6379:6379 redis
	docker run -d --name=postgres -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres
	function tearDown {
		docker stop test-redis postgres
		docker rm test-redis postgres
	}
	trap tearDown EXIT
	go test -v ./internal/...
	go test -v ./pkg/...
	go test -v ./cmd/...

e2e-create-cluster: build-docker-e2e-tests
	go get sigs.k8s.io/kind@v0.11.1
	kind create cluster --config e2e/setup/kind-confg-server.yaml
	kubectl apply -f ./e2e/setup/namespace-with-anonymous-user.yaml
	helm repo add nats https://nats-io.github.io/k8s/helm/charts
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm install redis bitnami/redis \
		--set architecture=standalone \
		--set auth.sentinel=false \
		--set auth.enabled=false \
		--wait
	helm install nats nats/stan --wait
	kind load docker-image tests/armada-server:latest --name e2e
	kind load docker-image tests/armada-executor:latest --name e2e	

e2e-install-armada:
	find . \( -name "Chart.yaml" -o -name "values.yaml" \) -exec sed -i s/LATEST/1.0.0/ {} +
	helm install -f e2e/setup/armada-server-e2e-values.yaml armada-server ./deployment/armada \
		--set image.repository=tests/armada-server \
		--set image.tag=latest \
		--wait
	helm install -f e2e/setup/executor-e2e-values.yaml armada-executor ./deployment/executor \
		--set applicationConfig.apiConnection.armadaUrl="armada:50051" \
		--set image.repository=tests/armada-executor \
		--set image.tag=latest \
		--wait

tests-e2e: e2e-create-cluster e2e-install-armada
	go get github.com/G-Research/armada/pkg/client
	INTEGRATION_ENABLED=true PATH=${PATH}:${PWD}/bin go test -v ./e2e/test/... -count=1

e2e-delete-cluster:
	kind delete cluster --name=e2e 

proto:
	docker build $(dockerFlags) -t armada-proto -f ./build/proto/Dockerfile .
	docker run -i --rm -v $(shell pwd):/go/src/armada -w /go/src/armada armada-proto ./scripts/proto.sh

generate:
	go run github.com/rakyll/statik \
		-dest=internal/lookout/repository/schema/ -src=internal/lookout/repository/schema/ -include=\*.sql -ns=lookout/sql -Z -f -m
	go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" internal/lookout/repository/schema/statik
