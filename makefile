# Determine which platform we're on based on the kernel name
platform := $(shell uname -s || echo unknown)

# Check that all necessary executables are present
# Using 'where' on Windows and 'which' on Unix-like systems, respectively
# We do not check for 'date', since it's a cmdlet on Windows, which do not show up with where
# (:= assignment is necessary to force immediate evaluation of expression)
EXECUTABLES = git docker
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

# GOPATH used for tests, which is mounted into the docker containers running the tests.
# If there's a GOPATH environment variable set, use that.
# Otherwise, if go is available on the host system, get the GOPATH via that.
# Otherwise, use ".go".
DOCKER_GOPATH = ${GOPATH}
ifeq ($(DOCKER_GOPATH),)
	DOCKER_GOPATH = $(shell go env GOPATH || echo "")
endif
ifeq ($(DOCKER_GOPATH),)
	DOCKER_GOPATH = .go
endif

# For reproducibility, run build commands in docker containers with known toolchain versions.
# INTEGRATION_ENABLED=true is needed for the e2e tests.
#
# For NuGet configuration, place a NuGet.Config in the project root directory.
# This file will get mounted into the container and used to configure NuGet.
#
# For npm, set the npm_config_disturl and npm_config_registry environment variables.
# Alternatively, place a .npmrc file in internal/lookout/ui
#
# To support SSL for alternate sources, we mount /etc/ssl/certs (which is Linux-specific) into the Docker container.
# If not using alternate sources, this mount can be removed.

# Deal with the fact that GOPATH might refer to multiple entries multiple directories
# For now just take the first one
DOCKER_GOPATH_TOKS := $(subst :, ,$(DOCKER_GOPATH:v%=%))
DOCKER_GOPATH_DIR = $(word 1,$(DOCKER_GOPATH_TOKS))

GO_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada --network=host \
	-e GOPROXY -e GOPRIVATE -e INTEGRATION_ENABLED=true -e CGO_ENABLED=0 -e GOOS=linux -e GARCH=amd64 \
	-v $(DOCKER_GOPATH_DIR):/go \
	golang:1.16-buster
DOTNET_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada \
	-v /etc/ssl/certs:/etc/ssl/certs \
	-e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
	dotnet/sdk:3.1.417-buster
NODE_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada/internal/lookout/ui \
	-e npm_config_disturl \
	-e npm_config_registry \
	-v /etc/ssl/certs:/etc/ssl/certs \
	-e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
	-e npm_config_cafile=/etc/ssl/certs/ca-certificates.crt \
	node:16.14-buster

# Optionally (if the TESTS_IN_DOCKER environment variable is set to true) run tests in docker containers.
# If using WSL, running tests in docker may result in network problems.
ifeq ($(TESTS_IN_DOCKER),true)
	GO_TEST_CMD = $(GO_CMD)
else
	GO_TEST_CMD =
endif

# Get go version from the local install
# (using subst to change, e.g., 'go version go1.17.2 windows/amd64' to 'go1.17.2 windows/amd64')
GO_VERSION_STRING = $(strip $(subst go version,,$(shell $(GO_CMD) go version)))

# Get most recent git commit (to insert into go build)
GIT_COMMIT := $(shell git rev-list --abbrev-commit -1 HEAD)

# The RELEASE_VERSION environment variable is set by circleci (to insert into go build and output filenames)
ifndef RELEASE_VERSION
override RELEASE_VERSION = UNKNOWN_VERSION
endif

# use bash for running:
export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit

gobuildlinux = go build -ldflags="-s -w"
gobuild = go build

build-server:
	$(GO_CMD) $(gobuild) -o ./bin/server cmd/armada/main.go

build-executor:
	$(GO_CMD) $(gobuild) -o ./bin/executor cmd/executor/main.go

build-fakeexecutor:
	$(GO_CMD) $(gobuild) -o ./bin/executor cmd/fakeexecutor/main.go

ARMADACTL_BUILD_PACKAGE := github.com/G-Research/armada/internal/armadactl/build
define ARMADACTL_LDFLAGS
-X '$(ARMADACTL_BUILD_PACKAGE).BuildTime=$(BUILD_TIME)' \
-X '$(ARMADACTL_BUILD_PACKAGE).ReleaseVersion=$(RELEASE_VERSION)' \
-X '$(ARMADACTL_BUILD_PACKAGE).GitCommit=$(GIT_COMMIT)' \
-X '$(ARMADACTL_BUILD_PACKAGE).GoVersion=$(GO_VERSION_STRING)'
endef
build-armadactl:
	$(GO_CMD) $(gobuild) -ldflags="$(ARMADACTL_LDFLAGS)" -o ./bin/armadactl cmd/armadactl/main.go

build-armadactl-multiplatform:
	go install github.com/mitchellh/gox@v1.0.1
	$(GO_CMD) gox -ldflags="$(ARMADACTL_LDFLAGS)" -output="./bin/{{.OS}}-{{.Arch}}/armadactl" -arch="amd64" -os="windows linux darwin" ./cmd/armadactl/

build-armadactl-release: build-armadactl-multiplatform
	mkdir ./dist || true
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-linux-amd64.tar.gz -C ./bin/linux-amd64/ armadactl
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-darwin-amd64.tar.gz -C ./bin/darwin-amd64/ armadactl
	zip -j ./dist/armadactl-$(RELEASE_VERSION)-windows-amd64.zip ./bin/windows-amd64/armadactl.exe

build-binoculars:
	$(GO_CMD) $(gobuild) -o ./bin/binoculars cmd/binoculars/main.go

build-load-tester:
	$(GO_CMD) $(gobuild) -o ./bin/armada-load-tester cmd/armada-load-tester/main.go

build: build-server build-executor build-fakeexecutor build-armadactl build-load-tester build-binoculars

build-docker-server:
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/server cmd/armada/main.go
	docker build $(dockerFlags) -t armada -f ./build/armada/Dockerfile .

build-docker-executor:
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/executor cmd/executor/main.go
	docker build $(dockerFlags) -t armada-executor -f ./build/executor/Dockerfile .

build-docker-armada-load-tester:
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/armada-load-tester cmd/armada-load-tester/main.go
	docker build $(dockerFlags) -t armada-load-tester -f ./build/armada-load-tester/Dockerfile .

build-docker-armadactl:
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/armadactl cmd/armadactl/main.go
	docker build $(dockerFlags) -t armadactl -f ./build/armadactl/Dockerfile .

build-docker-fakeexecutor:
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/fakeexecutor cmd/fakeexecutor/main.go
	docker build $(dockerFlags) -t armada-fakeexecutor -f ./build/fakeexecutor/Dockerfile .

build-docker-lookout:
	$(NODE_CMD) npm ci
	# The following line is equivalent to running "npm run openapi".
	# We use this instead of "npm run openapi" since if NODE_CMD is set to run npm in docker,
	# "npm run openapi" would result in running a docker container in docker.
	docker run --rm -u $(id -u ${USER}):$(id -g ${USER}) -v ${PWD}:/project openapitools/openapi-generator-cli:v5.2.0 /project/internal/lookout/ui/openapi.sh
	$(NODE_CMD) npm run build
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/lookout cmd/lookout/main.go
	docker build $(dockerFlags) -t armada-lookout -f ./build/lookout/Dockerfile .

build-docker-binoculars:
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/binoculars cmd/binoculars/main.go
	docker build $(dockerFlags) -t armada-binoculars -f ./build/binoculars/Dockerfile .

build-docker: build-docker-server build-docker-executor build-docker-armadactl build-docker-armada-load-tester build-docker-fakeexecutor build-docker-lookout build-docker-binoculars

# Build target without lookout (to avoid needing to load npm packages from the Internet).
build-docker-no-lookout: build-docker-server build-docker-executor build-docker-armadactl build-docker-armada-load-tester build-docker-fakeexecutor build-docker-binoculars

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-armadactl-multiplatform build-load-tester

.ONESHELL:
tests-teardown:
	docker rm -f redis postgres || true

.ONESHELL:
tests:
	mkdir -p test_reports
	docker run -d --name=redis --network=host -p=6379:6379 redis
	docker run -d --name=postgres --network=host -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres
	function tearDown {
		docker rm -f redis postgres
	}
	trap tearDown EXIT
	$(GO_TEST_CMD) go test -v ./internal... 2>&1 | tee test_reports/internal.txt
	$(GO_TEST_CMD) go test -v ./pkg... 2>&1 | tee test_reports/pkg.txt
	$(GO_TEST_CMD) go test -v ./cmd... 2>&1 | tee test_reports/cmd.txt

# To be used during development to restart the Armada server before re-running tests.
.ONESHELL:
rebuild-server:
	docker rm -f server
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/server cmd/armada/main.go
	docker build $(dockerFlags) -t armada -f ./build/armada/Dockerfile .
	docker run -d --name server --network=kind -p=50051:50051 -p 8080:8080 -v ${PWD}/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/nats/armada-config.yaml --config /e2e/setup/redis/armada-config.yaml --config /e2e/setup/pulsar/armada-config.yaml  --config /e2e/setup/server/armada-config.yaml

.ONESHELL:
tests-e2e-teardown:
	docker rm -f nats redis pulsar server executor postgres || true
	kind delete cluster --name armada-test || true
	rm .kube/config || true
	rmdir .kube || true

tests-e2e-setup:
	docker pull "alpine:3.10" # ensure Alpine, which is used by tests, is available
	kind create cluster --name armada-test --wait 30s --image kindest/node:v1.21.1
	kind load docker-image "alpine:3.10" --name armada-test # needed to make Alpine available to kind

	mkdir -p .kube
	kind get kubeconfig --internal --name armada-test > .kube/config

	docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada -e KUBECONFIG=/go/src/armada/.kube/config --network kind bitnami/kubectl:1.23 apply -f ./e2e/setup/namespace-with-anonymous-user.yaml
	docker run -d --name nats --network=kind nats-streaming
	docker run -d --name redis -p=6379:6379 --network=kind redis
	docker run -d --name pulsar -p 0.0.0.0:6650:6650 --network=kind apachepulsar/pulsar:2.9.1 bin/pulsar standalone
	docker run -d --name postgres --network=kind -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres

	sleep 10 # give dependencies time to start up
	docker run -d --name server --network=kind -p=50051:50051 -p 8080:8080 -v ${PWD}/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/nats/armada-config.yaml --config /e2e/setup/redis/armada-config.yaml --config /e2e/setup/pulsar/armada-config.yaml  --config /e2e/setup/server/armada-config.yaml
	docker run -d --name executor --network=kind -v ${PWD}/.kube:/.kube -v ${PWD}/e2e:/e2e  \
		-e KUBECONFIG=/.kube/config \
		-e ARMADA_KUBERNETES_IMPERSONATEUSERS=true \
		-e ARMADA_KUBERNETES_STUCKPODEXPIRY=15s \
		-e ARMADA_APICONNECTION_ARMADAURL="server:50051" \
		-e ARMADA_APICONNECTION_FORCENOTLS=true \
		armada-executor --config /e2e/setup/executor-config.yaml

.ONESHELL:
tests-e2e-no-setup:
	mkdir -p test_reports
	$(GO_TEST_CMD) go test -v ./e2e/armadactl_test/... -count=1 2>&1 | tee test_reports/e2e_armadactl.txt
	$(GO_TEST_CMD) go test -v ./e2e/basic_test/... -count=1 2>&1 | tee test_reports/e2e_basic.txt
	$(GO_TEST_CMD) go test -v ./e2e/pulsar_test/... -count=1 2>&1 | tee test_reports/e2e_pulsar.txt
	# $(DOTNET_CMD) dotnet test client/DotNet/Armada.Client.Test/Armada.Client.Test.csproj

.ONESHELL:
tests-e2e: build-armadactl build-docker-no-lookout tests-e2e-setup
	function teardown {
		echo -e "\nexecutor logs:"
		docker logs executor
		echo -e "\nserver logs:"
		docker logs server
		docker rm -f nats redis pulsar server executor postgres
		kind delete cluster --name armada-test
		rm .kube/config
		rmdir .kube
	}
	mkdir -p test_reports
	trap teardown exit
	sleep 10
	echo -e "\nrunning tests:"
	$(GO_TEST_CMD) go test -v ./e2e/armadactl_test/... -count=1 2>&1 | tee test_reports/e2e_armadactl.txt
	$(GO_TEST_CMD) go test -v ./e2e/basic_test/... -count=1 2>&1 | tee test_reports/e2e_basic.txt
	$(GO_TEST_CMD) go test -v ./e2e/pulsar_test/... -count=1 2>&1 | tee test_reports/e2e_pulsar.txt
	# $(DOTNET_CMD) dotnet test client/DotNet/Armada.Client.Test/Armada.Client.Test.csproj

# Output test results in Junit format, e.g., to display in Jenkins.
# Relies on go-junit-report
# https://github.com/jstemmer/go-junit-report
junit-report:
	mkdir -p test_reports
	sync # make sure everything has been synced to disc
	rm -f test_reports/junit.xml
	$(GO_TEST_CMD) bash -c "cat test_reports/*.txt | go-junit-report > test_reports/junit.xml"

proto:
	docker build $(dockerFlags) -t armada-proto -f ./build/proto/Dockerfile .
	docker run -it --rm -v $(shell pwd):/go/src/armada -w /go/src/armada armada-proto ./scripts/proto.sh

# Target for compiling the dotnet Armada client.
dotnet:
	$(DOTNET_CMD) dotnet build ./client/DotNet/Armada.Client /t:NSwag

# Download all dependencies and install tools listed in internal/tools/tools.go
download:
	$(GO_CMD) go mod download
	$(GO_TEST_CMD) go mod download
	$(GO_CMD) go list -f '{{range .Imports}}{{.}} {{end}}' internal/tools/tools.go | xargs $(GO_CMD) go install
	$(GO_TEST_CMD) go list -f '{{range .Imports}}{{.}} {{end}}' internal/tools/tools.go | xargs $(GO_TEST_CMD) go install
	$(GO_CMD) go mod tidy
	$(GO_TEST_CMD) go mod tidy

code-reports:
	mkdir -p code_reports
	$(GO_CMD) goimports -d -local "github.com/G-Research/armada" . | tee code_reports/goimports.txt
	$(GO_CMD) ineffassign ./... | tee code_reports/ineffassign.txt

code-checks: code-reports
	sync # make sure everything has been synced to disc
	if [ $(shell cat code_reports/ineffassign.txt | wc -l) -ne "0" ]; then exit 1; fi
	if [ $(shell cat code_reports/goimports.txt | wc -l) -ne "0" ]; then exit 1; fi

generate:
	$(GO_CMD) go run github.com/rakyll/statik \
		-dest=internal/lookout/repository/schema/ -src=internal/lookout/repository/schema/ -include=\*.sql -ns=lookout/sql -Z -f -m && \
		go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" internal/lookout/repository/schema/statik
