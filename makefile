# Determine which platform we're on based on the kernel name
platform := $(shell uname -s || echo unknown)
host_arch := $(shell uname -m)
PWD := $(shell pwd)
# Check that all necessary executables are present
# Using 'where' on Windows and 'which' on Unix-like systems, respectively
# We do not check for 'date', since it's a cmdlet on Windows, which do not show up with where
# (:= assignment is necessary to force immediate evaluation of expression)
EXECUTABLES = git docker kubectl
ifeq ($(platform),windows32)
	K := $(foreach exec,$(EXECUTABLES),$(if $(shell where $(exec)),some string,$(error "No $(exec) in PATH")))
else
	K := $(foreach exec,$(EXECUTABLES),$(if $(shell which $(exec)),some string,$(error "No $(exec) in PATH")))
endif

# Docker buildkit builds work in parallel, lowering build times. Outputs are compatable.
# Ignored on docker <18.09. This may lead to slower builds and different logs in STDOUT,
# but the image output is the same.
# See https://docs.docker.com/develop/develop-images/build_enhancements/ for more info.
export DOCKER_BUILDKIT = 1

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

ifeq ($(platform),Darwin)
	DOCKER_NET =
else
	DOCKER_NET = --network=host
endif

ifeq ($(host_arch),arm64)
	PROTO_DOCKERFILE = ./build/proto/Dockerfile.arm64
else
	PROTO_DOCKERFILE = ./build/proto/Dockerfile
endif

ifeq ($(DOCKER_RUN_AS_USER),)
	DOCKER_RUN_AS_USER = -u $(shell id -u):$(shell id -g)
endif
ifeq ($(platform),windows32)
	DOCKER_RUN_AS_USER =
endif

# For reproducibility, run build commands in docker containers with known toolchain versions.
# INTEGRATION_ENABLED=true is needed for the e2e tests.
#
# For NuGet configuration, place a NuGet.Config in the project root directory.
# This file will get mounted into the container and used to configure NuGet.
#
# For npm, set the npm_config_disturl and npm_config_registry environment variables.
# Alternatively, place a .npmrc file in internal/lookout/ui

# Deal with the fact that GOPATH might refer to multiple entries multiple directories
# For now just take the first one
DOCKER_GOPATH_TOKS := $(subst :, ,$(DOCKER_GOPATH:v%=%))
DOCKER_GOPATH_DIR = $(word 1,$(DOCKER_GOPATH_TOKS))

# This is used to generate published artifacts; to raise the golang version we publish and run
# tests-in-docker against, you have to update the tag of the image used here.
GO_CMD = docker run --rm $(DOCKER_RUN_AS_USER) -v ${PWD}:/go/src/armada -w /go/src/armada $(DOCKER_NET) \
	-e GOPROXY -e GOPRIVATE -e GOCACHE=/go/cache -e INTEGRATION_ENABLED=true -e CGO_ENABLED=0 -e GOOS=linux -e GARCH=amd64 \
	-v $(DOCKER_GOPATH_DIR):/go \
	golang:1.20.2-buster

# Versions of third party API
# Bump if you are updating
GRPC_GATEWAY_VERSION:=@v1.16.0
GOGO_PROTOBUF_VERSION=@v1.3.2
K8_APIM_VERSION = @v0.22.4
K8_API_VERSION = @v0.22.4

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

# The RELEASE_TAG environment variable is set by circleci (to insert into go build and output filenames)
ifndef RELEASE_TAG
override RELEASE_TAG = UNKNOWN_TAG
endif

# The NUGET_API_KEY environment variable is set by circleci (to insert into dotnet nuget push commands)
ifndef NUGET_API_KEY
override NUGET_API_KEY = UNKNOWN_NUGET_API_KEY
endif

# use bash for running:
export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit

gobuildlinux = go build -ldflags="-s -w"
gobuild = go build

NODE_DOCKER_IMG := node:16.14-buster
DOTNET_DOCKER_IMG := mcr.microsoft.com/dotnet/sdk:3.1.417-buster

# By default, the install process trusts standard SSL root CAs only.
# To use your host system's SSL certs on Debian/Ubuntu, AmazonLinux, or MacOS, uncomment the line below
#
# USE_SYSTEM_CERTS := true

ifdef USE_SYSTEM_CERTS

DOTNET_CMD = docker run -v ${PWD}:/go/src/armada -w /go/src/armada \
	-v ${PWD}/build/ssl/certs/:/etc/ssl/certs \
	-e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
	${DOTNET_DOCKER_IMG}

NODE_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada/internal/lookout/ui \
	-e npm_config_disturl \
	-e npm_config_registry \
	-v build/ssl/certs/:/etc/ssl/certs \
	-e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
	-e npm_config_cafile=/etc/ssl/certs/ca-certificates.crt \
	${NODE_DOCKER_IMG}

UNAME_S := $(shell uname -s)
ssl-certs:
	mkdir -p build/ssl/certs/
	rm -f build/ssl/certs/ca-certificates.crt
 ifneq ("$(wildcard /etc/ssl/certs/ca-certificates.crt)", "")
	# Debian-based distros
	cp /etc/ssl/certs/ca-certificates.crt build/ssl/certs/ca-certificates.crt
 else ifneq ("$(wildcard /etc/ssl/certs/ca-bundle.crt)","")
	# AmazonLinux
	cp /etc/ssl/certs/ca-bundle.crt build/ssl/certs/ca-certificates.crt
 else ifeq ("$(UNAME_S)", "Darwin")
	# MacOS
	security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain >> build/ssl/certs/ca-certificates.crt
	security find-certificate -a -p /Library/Keychains/System.keychain >> build/ssl/certs/ca-certificates.crt
	security find-certificate -a -p ~/Library/Keychains/login.keychain-db >> build/ssl/certs/ca-certificates.crt
 else
	echo "Don't know where to find root CA certs"
	exit 1
 endif

node-setup: ssl-certs
dotnet-setup: ssl-certs

else

DOTNET_CMD = docker run -v ${PWD}:/go/src/armada -w /go/src/armada \
	${DOTNET_DOCKER_IMG}

NODE_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada/internal/lookout/ui \
	-e npm_config_disturl \
	-e npm_config_registry \
	${NODE_DOCKER_IMG}

# no setup necessary for node or dotnet if using default SSL certs
node-setup:
dotnet-setup:
endif

build-server:
	$(GO_CMD) $(gobuild) -o ./bin/server cmd/armada/main.go

build-executor:
	$(GO_CMD) $(gobuild) -o ./bin/executor cmd/executor/main.go

build-fakeexecutor:
	$(GO_CMD) $(gobuild) -o ./bin/executor cmd/fakeexecutor/main.go

ARMADACTL_BUILD_PACKAGE := github.com/armadaproject/armada/internal/armadactl/build
define ARMADACTL_LDFLAGS
-X '$(ARMADACTL_BUILD_PACKAGE).BuildTime=$(BUILD_TIME)' \
-X '$(ARMADACTL_BUILD_PACKAGE).ReleaseVersion=$(RELEASE_VERSION)' \
-X '$(ARMADACTL_BUILD_PACKAGE).GitCommit=$(GIT_COMMIT)' \
-X '$(ARMADACTL_BUILD_PACKAGE).GoVersion=$(GO_VERSION_STRING)'
endef
build-armadactl:
	$(GO_CMD) $(gobuild) -ldflags="$(ARMADACTL_LDFLAGS)" -o ./bin/armadactl cmd/armadactl/main.go

build-armadactl-multiplatform:
	$(GO_CMD) gox -ldflags="$(ARMADACTL_LDFLAGS)" -output="./bin/{{.OS}}-{{.Arch}}/armadactl" -arch="amd64" -os="windows linux darwin" ./cmd/armadactl/

build-armadactl-release: build-armadactl-multiplatform
	mkdir ./dist || true
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-linux-amd64.tar.gz -C ./bin/linux-amd64/ armadactl
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-darwin-amd64.tar.gz -C ./bin/darwin-amd64/ armadactl
	zip -j ./dist/armadactl-$(RELEASE_VERSION)-windows-amd64.zip ./bin/windows-amd64/armadactl.exe

PULSARTEST_BUILD_PACKAGE := github.com/armadaproject/armada/internal/pulsartest/build
define PULSARTEST_LDFLAGS
-X '$(PULSARTEST_BUILD_PACKAGE).BuildTime=$(BUILD_TIME)' \
-X '$(PULSARTEST_BUILD_PACKAGE).ReleaseVersion=$(RELEASE_VERSION)' \
-X '$(PULSARTEST_BUILD_PACKAGE).GitCommit=$(GIT_COMMIT)' \
-X '$(PULSARTEST_BUILD_PACKAGE).GoVersion=$(GO_VERSION_STRING)'
endef
build-pulsartest:
	$(GO_CMD) $(gobuild) -ldflags="$(PULSARTEST_LDFLAGS)" -o ./bin/pulsartest cmd/pulsartest/main.go

TESTSUITE_BUILD_PACKAGE := github.com/armadaproject/armada/internal/testsuite/build
define TESTSUITE_LDFLAGS
-X '$(TESTSUITE_BUILD_PACKAGE).BuildTime=$(BUILD_TIME)' \
-X '$(TESTSUITE_BUILD_PACKAGE).ReleaseVersion=$(RELEASE_VERSION)' \
-X '$(TESTSUITE_BUILD_PACKAGE).GitCommit=$(GIT_COMMIT)' \
-X '$(TESTSUITE_BUILD_PACKAGE).GoVersion=$(GO_VERSION_STRING)'
endef
build-testsuite:
	$(GO_CMD) $(gobuild) -ldflags="$(TESTSUITE_LDFLAGS)" -o ./bin/testsuite cmd/testsuite/main.go

build-binoculars:
	$(GO_CMD) $(gobuild) -o ./bin/binoculars cmd/binoculars/main.go

build-load-tester:
	$(GO_CMD) $(gobuild) -o ./bin/armada-load-tester cmd/armada-load-tester/main.go

build-lookout-ingester:
	$(GO_CMD) $(gobuild) -o ./bin/lookoutingester cmd/lookoutingester/main.go

build-event-ingester:
	$(GO_CMD) $(gobuild) -o ./bin/eventingester cmd/eventingester/main.go

build-jobservice:
	$(GO_CMD) $(gobuild) -o ./bin/jobservice cmd/jobservice/main.go

build-lookout:
	$(GO_CMD) $(gobuild) -o ./bin/lookoutingester cmd/lookoutingester/main.go

build-lookoutv2:
	$(GO_CMD) $(gobuild) -o ./bin/lookoutv2 cmd/lookoutv2/main.go

build lookoutingesterv2:
	$(GO_CMD) $(gobuild) -o ./bin/lookoutingesterv2 cmd/lookoutingesterv2/main.go

build: build lookoutingesterv2 build-lookoutv2 build-lookout build-jobservice build-server build-executor build-fakeexecutor build-armadactl build-load-tester build-testsuite build-binoculars build-lookout-ingester build-event-ingester

build-docker-server:
	mkdir -p .build/server
	$(GO_CMD) $(gobuildlinux) -o ./.build/server/server cmd/armada/main.go
	cp -a ./config/armada ./.build/server/config
	docker buildx build -o type=docker $(dockerFlags) -t armada -f ./build/armada/Dockerfile ./.build/server/

build-docker-executor:
	mkdir -p .build/executor
	$(GO_CMD) $(gobuildlinux) -o ./.build/executor/executor cmd/executor/main.go
	cp -a ./config/executor ./.build/executor/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-executor -f ./build/executor/Dockerfile ./.build/executor

build-docker-armada-load-tester:
	mkdir -p .build/armada-load-tester
	$(GO_CMD) $(gobuildlinux) -o ./.build/armada-load-tester/armada-load-tester cmd/armada-load-tester/main.go
	docker buildx build -o type=docker $(dockerFlags) -t armada-load-tester -f ./build/armada-load-tester/Dockerfile ./.build/armada-load-tester

build-docker-testsuite:
	mkdir -p .build/testsuite
	$(GO_CMD) $(gobuildlinux) -ldflags="$(TESTSUITE_LDFLAGS)" -o ./.build/testsuite/testsuite cmd/testsuite/main.go
	docker buildx build -o type=docker $(dockerFlags) -t testsuite -f ./build/testsuite/Dockerfile ./.build/testsuite

build-docker-armadactl:
	mkdir -p .build/armadactl
	$(GO_CMD) $(gobuildlinux) -ldflags="$(ARMADACTL_LDFLAGS)" -o ./.build/armadactl/armadactl cmd/armadactl/main.go
	docker buildx build -o type=docker $(dockerFlags) -t armadactl -f ./build/armadactl/Dockerfile ./.build/armadactl

build-docker-fakeexecutor:
	mkdir -p .build/fakeexecutor
	$(GO_CMD) $(gobuildlinux) -o ./.build/fakeexecutor/fakeexecutor cmd/fakeexecutor/main.go
	cp -a ./config/executor ./.build/fakeexecutor/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-fakeexecutor -f ./build/fakeexecutor/Dockerfile ./.build/fakeexecutor

build-docker-lookout-ingester:
	mkdir -p .build/lookoutingester
	$(GO_CMD) $(gobuildlinux) -o ./.build/lookoutingester/lookoutingester cmd/lookoutingester/main.go
	cp -a ./config/lookoutingester ./.build/lookoutingester/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-lookout-ingester -f ./build/lookoutingester/Dockerfile ./.build/lookoutingester

build-docker-lookout-ingester-v2:
	mkdir -p .build/lookoutingesterv2
	$(GO_CMD) $(gobuildlinux) -o ./.build/lookoutingesterv2/lookoutingesterv2 cmd/lookoutingesterv2/main.go
	cp -a ./config/lookoutingesterv2 ./.build/lookoutingesterv2/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-lookout-ingester-v2 -f ./build/lookoutingesterv2/Dockerfile ./.build/lookoutingesterv2

build-docker-event-ingester:
	mkdir -p .build/eventingester
	$(GO_CMD) $(gobuildlinux) -o ./.build/eventingester/eventingester cmd/eventingester/main.go
	cp -a ./config/eventingester ./.build/eventingester/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-event-ingester -f ./build/eventingester/Dockerfile ./.build/eventingester

build-docker-lookout: node-setup
	$(NODE_CMD) yarn install --immutable
	# The following line is equivalent to running "yarn run openapi".
	# We use this instead of "yarn run openapi" since if NODE_CMD is set to run npm in docker,
	# "yarn run openapi" would result in running a docker container in docker.
	docker run --rm $(DOCKER_RUN_AS_USER) -v ${PWD}:/project openapitools/openapi-generator-cli:v5.4.0 /project/internal/lookout/ui/openapi.sh
	$(NODE_CMD) yarn run build
	$(GO_CMD) $(gobuildlinux) -o ./bin/linux/lookout cmd/lookout/main.go
	docker buildx build -o type=docker $(dockerFlags) -t armada-lookout -f ./build/lookout/Dockerfile .

build-docker-lookout-v2:
	mkdir -p .build/lookoutv2
	$(GO_CMD) $(gobuildlinux) -o ./.build/lookoutv2/lookoutv2 cmd/lookoutv2/main.go
	cp -a ./config/lookoutv2 ./.build/lookoutv2/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-lookout-v2 -f ./build/lookoutv2/Dockerfile ./.build/lookoutv2

build-docker-binoculars:
	mkdir -p .build/binoculars
	$(GO_CMD) $(gobuildlinux) -o ./.build/binoculars/binoculars cmd/binoculars/main.go
	cp -a ./config/binoculars ./.build/binoculars/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-binoculars -f ./build/binoculars/Dockerfile ./.build/binoculars

build-docker-jobservice:
	mkdir -p .build/jobservice
	$(GO_CMD) $(gobuildlinux) -o ./.build/jobservice/jobservice cmd/jobservice/main.go
	cp -a ./config/jobservice ./.build/jobservice/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-jobservice -f ./build/jobservice/Dockerfile ./.build/jobservice

build-docker-scheduler:
	mkdir -p .build/scheduler
	$(GO_CMD) $(gobuildlinux) -o ./.build/scheduler/scheduler cmd/scheduler/main.go
	cp -a ./config/scheduler ./.build/scheduler/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-scheduler -f ./build/scheduler/Dockerfile ./.build/scheduler

build-docker-scheduler-ingester:
	mkdir -p .build/scheduleringester
	$(GO_CMD) $(gobuildlinux) -o ./.build/scheduleringester/scheduleringester cmd/scheduleringester/main.go
	cp -a ./config/scheduleringester ./.build/scheduleringester/config
	docker buildx build -o type=docker $(dockerFlags) -t armada-scheduler-ingester -f ./build/scheduleringester/Dockerfile ./.build/scheduleringester

build-docker-full-bundle: build
	ls -la ./bin
	cp -a ./bin/server ./server
	cp -a ./bin/executor ./executor
	cp -a ./bin/lookoutingester ./lookoutingester
	cp -a ./bin/lookoutingesterv2 ./lookoutingesterv2
	cp -a ./bin/eventingester ./eventingester
	cp -a ./bin/binoculars ./binoculars
	cp -a ./bin/jobservice ./jobservice
	cp -a ./bin/lookout ./lookout
	cp -a ./bin/lookoutv2 ./lookoutv2

	docker buildx build -o type=docker $(dockerFlags) -t armada-full-bundle -f ./build_goreleaser/bundles/full/Dockerfile .

build-docker: build-docker-no-lookout build-docker-lookout build-docker-lookout-v2

# Build target without lookout (to avoid needing to load npm packages from the Internet).
# We still build lookout-ingester since that go code that is isolated from lookout itself.
build-docker-no-lookout: build-docker-server build-docker-executor build-docker-armadactl build-docker-testsuite build-docker-armada-load-tester build-docker-fakeexecutor build-docker-binoculars build-docker-lookout-ingester build-docker-lookout-ingester-v2 build-docker-event-ingester build-docker-jobservice build-docker-scheduler build-docker-scheduler-ingester

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-armadactl-multiplatform build-load-tester build-testsuite

.ONESHELL:
tests-teardown:
	docker rm -f redis postgres || true

.ONESHELL:
tests-no-setup: gotestsum
	$(GOTESTSUM) -- -v ./internal... 2>&1 | tee test_reports/internal.txt
	$(GOTESTSUM) -- -v ./pkg... 2>&1 | tee test_reports/pkg.txt
	$(GOTESTSUM) -- -v ./cmd... 2>&1 | tee test_reports/cmd.txt

.ONESHELL:
tests: gotestsum
	mkdir -p test_reports
	docker run -d --name=redis $(DOCKER_NET) -p=6379:6379 redis:6.2.6
	docker run -d --name=postgres $(DOCKER_NET) -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres:14.2
	sleep 3
	function tearDown { docker rm -f redis postgres; }; trap tearDown EXIT
	$(GOTESTSUM) -- -coverprofile internal_coverage.xml -v ./internal... 2>&1 | tee test_reports/internal.txt
	$(GOTESTSUM) -- -coverprofile pkg_coverage.xml -v ./pkg... 2>&1 | tee test_reports/pkg.txt
	$(GOTESTSUM) -- -coverprofile cmd_coverage.xml -v ./cmd... 2>&1 | tee test_reports/cmd.txt

.ONESHELL:
lint-fix:
	$(GO_TEST_CMD) golangci-lint run --fix --timeout 10m

.ONESHELL:
lint:
	$(GO_TEST_CMD) golangci-lint run --timeout 10m

.ONESHELL:
code-checks: lint

# Rebuild and restart the server.
.ONESHELL:
rebuild-server: build-docker-server
	docker rm -f server || true
	docker run -d --name server --network=kind -p=50051:50051 -p 8080:8080 -v ${PWD}/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/redis/armada-config.yaml --config /e2e/setup/pulsar/armada-config.yaml --config /e2e/setup/server/armada-config.yaml

# Rebuild and restart the executor.
.ONESHELL:
rebuild-executor: build-docker-executor
	docker rm -f executor || true
	docker run -d --name executor --network=kind -v ${PWD}/.kube:/.kube -v ${PWD}/e2e:/e2e  \
		-e KUBECONFIG=/.kube/config \
		-e ARMADA_KUBERNETES_IMPERSONATEUSERS=true \
		-e ARMADA_KUBERNETES_STUCKPODEXPIRY=15s \
		-e ARMADA_APICONNECTION_ARMADAURL="server:50051" \
		-e ARMADA_APICONNECTION_FORCENOTLS=true \
		armada-executor --config /e2e/setup/insecure-executor-config.yaml

.ONESHELL:
tests-e2e-teardown:
	docker rm -f redis pulsar server executor postgres lookout-ingester-migrate lookout-ingester jobservice event-ingester || true
	kind delete cluster --name armada-test || true
	rm .kube/config || true
	rmdir .kube || true

.ONESHELL:
setup-cluster:
	kind create cluster --config e2e/setup/kind.yaml

	# Load images necessary for tests.
	docker pull "alpine:3.10" # used for e2e tests
	docker pull "nginx:1.21.6" # used for e2e tests (ingress)
	docker pull "registry.k8s.io/ingress-nginx/controller:v1.4.0"
	docker pull "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20220916-gd32f8c343"
	kind load docker-image "alpine:3.10" --name armada-test
	kind load docker-image "nginx:1.21.6" --name armada-test
	kind load docker-image "registry.k8s.io/ingress-nginx/controller:v1.4.0" --name armada-test
	kind load docker-image "registry.k8s.io/ingress-nginx/kube-webhook-certgen:v20220916-gd32f8c343" --name armada-test

	# Ingress controller needed for cluster ingress.
	kubectl apply -f e2e/setup/ingress-nginx.yaml --context kind-armada-test

	# Priority classes.
	kubectl apply -f e2e/setup/priorityclasses.yaml --context kind-armada-test

	# Wait until the ingress controller is ready
	echo "Waiting for ingress controller to become ready"
	sleep 10 # calling wait immediately can result in "no matching resources found"
	kubectl wait --namespace ingress-nginx \
		--for=condition=ready pod \
		--selector=app.kubernetes.io/component=controller \
		--timeout=90s \
		--context kind-armada-test

	mkdir -p .kube
	kind get kubeconfig --internal --name armada-test > .kube/config

tests-e2e-setup: setup-cluster
	docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada -e KUBECONFIG=/go/src/armada/.kube/config --network kind bitnami/kubectl:1.24.8 apply -f ./e2e/setup/namespace-with-anonymous-user.yaml

	# Armada dependencies.
	docker run -d --name pulsar -p 0.0.0.0:6650:6650 --network=kind apachepulsar/pulsar:2.9.2 bin/pulsar standalone
	docker run -d --name redis -p=6379:6379 --network=kind redis:6.2.6
	docker run -d --name postgres --network=kind -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres:14.2

	sleep 60 # give dependencies time to start up
	docker run -d --name server --network=kind -p=50051:50051 -p 8080:8080 -v ${PWD}/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/redis/armada-config.yaml --config /e2e/setup/pulsar/armada-config.yaml  --config /e2e/setup/server/armada-config.yaml
	docker run -d --name executor --network=kind -v ${PWD}/.kube:/.kube -v ${PWD}/e2e:/e2e  \
		-e KUBECONFIG=/.kube/config \
		-e ARMADA_KUBERNETES_IMPERSONATEUSERS=true \
		-e ARMADA_KUBERNETES_STUCKPODEXPIRY=15s \
		-e ARMADA_APICONNECTION_ARMADAURL="server:50051" \
		-e ARMADA_APICONNECTION_FORCENOTLS=true \
		armada-executor --config /e2e/setup/insecure-executor-config.yaml
	docker run -d --name lookout-ingester-migrate  --network=kind -v ${PWD}/e2e:/e2e \
		armada-lookout-ingester --config /e2e/setup/lookout-ingester-config.yaml --migrateDatabase
	docker run -d --name lookout-ingester --network=kind -v ${PWD}/e2e:/e2e \
		armada-lookout-ingester --config /e2e/setup/lookout-ingester-config.yaml
	docker run -d --name event-ingester --network=kind -v ${PWD}/e2e:/e2e armada-event-ingester
	docker run -d --name jobservice --network=kind -v ${PWD}/e2e:/e2e \
	    armada-jobservice run --config /e2e/setup/jobservice.yaml

	# Create test queue if it doesn't already exist
	$(GO_CMD) go run cmd/armadactl/main.go create queue e2e-test-queue || true
	$(GO_CMD) go run cmd/armadactl/main.go create queue queue-a || true
	$(GO_CMD) go run cmd/armadactl/main.go create queue queue-b || true

	# Logs to diagonose problems
	docker logs executor
	docker logs server
.ONESHELL:
tests-e2e-no-setup: gotestsum
	function printApplicationLogs {
		echo -e "\nexecutor logs:"
		docker logs executor
		echo -e "\nserver logs:"
		docker logs server
	}
	trap printApplicationLogs exit
	mkdir -p test_reports
	$(GOTESTSUM) -- -v ./e2e/armadactl_test/... -count=1 2>&1 | tee test_reports/e2e_armadactl.txt
	$(GOTESTSUM) -- -v ./e2e/pulsar_test/... -count=1 2>&1 | tee test_reports/e2e_pulsar.txt
	$(GOTESTSUM) -- -v ./e2e/pulsartest_client/... -count=1 2>&1 | tee test_reports/e2e_pulsartest_client.txt
	$(GOTESTSUM) -- -v ./e2e/lookout_ingester_test/... -count=1 2>&1 | tee test_reports/e2e_lookout_ingester.txt
	# $(DOTNET_CMD) dotnet test client/DotNet/Armada.Client.Test/Armada.Client.Test.csproj

.ONESHELL:
tests-e2e: build-armadactl build-docker-no-lookout tests-e2e-setup gotestsum
	function teardown {
		echo -e "\nexecutor logs:"
		docker logs executor
		echo -e "\nserver logs:"
		docker logs server
		docker rm -f redis pulsar server executor postgres lookout-ingester-migrate lookout-ingester event-ingester jobservice
		kind delete cluster --name armada-test
		rm .kube/config
		rmdir .kube
	}
	mkdir -p test_reports
	trap teardown exit
	sleep 10
	echo -e "\nrunning tests:"
	$(GOTESTSUM) -- -v ./e2e/armadactl_test/... -count=1 2>&1 | tee test_reports/e2e_armadactl.txt
	$(GOTESTSUM) -- -v ./e2e/basic_test/... -count=1 2>&1 | tee test_reports/e2e_basic.txt
	$(GOTESTSUM) -- -v ./e2e/pulsar_test/... -count=1 2>&1 | tee test_reports/e2e_pulsar.txt
	$(GOTESTSUM) -- -v ./e2e/pulsartest_client/... -count=1 2>&1 | tee test_reports/e2e_pulsartest_client.txt
	$(GOTESTSUM) -- -v ./e2e/lookout_ingester_test/... -count=1 2>&1 | tee test_reports/e2e_lookout_ingester.txt

	# $(DOTNET_CMD) dotnet test client/DotNet/Armada.Client.Test/Armada.Client.Test.csproj
.ONESHELL:
tests-e2e-python: python
	docker run -v${PWD}/client/python:/code --workdir /code -e ARMADA_SERVER=server -e ARMADA_PORT=50051 --entrypoint python3 --network=kind armada-python-client-builder:latest -m pytest -v -s /code/tests/integration/test_no_auth.py

# To run integration tests with jobservice and such, we can run this command
# For now, let's just have it in rare cases that people need to test.
# You must have an existing cluster working to run this command.
.ONESHELL:
tests-e2e-airflow: airflow-operator build-docker-jobservice
	$(GO_CMD) go run cmd/armadactl/main.go create queue queue-a || true
	docker rm -f jobservice || true
	docker run -d --name jobservice --network=kind --mount 'type=bind,src=${PWD}/e2e,dst=/e2e' armada-jobservice run --config /e2e/setup/jobservice.yaml
	docker run -v ${PWD}/e2e:/e2e -v ${PWD}/third_party/airflow:/code --workdir /code -e ARMADA_SERVER=server -e ARMADA_PORT=50051 -e JOB_SERVICE_HOST=jobservice -e JOB_SERVICE_PORT=60003 --entrypoint python3 --network=kind armada-airflow-operator-builder:latest -m pytest -v -s /code/tests/integration/test_airflow_operator_logic.py
	docker rm -f jobservice

# Output test results in Junit format, e.g., to display in Jenkins.
# Relies on go-junit-report
# https://github.com/jstemmer/go-junit-report
junit-report:
	mkdir -p test_reports
	sync # make sure everything has been synced to disc
	rm -f test_reports/junit.xml
	$(GO_TEST_CMD) bash -c "cat test_reports/*.txt | go-junit-report > test_reports/junit.xml"

python: proto-setup
	docker buildx build -o type=docker $(dockerFlags) -t armada-python-client-builder -f ./build/python-client/Dockerfile .
	docker run --rm -v ${PWD}/proto:/proto -v ${PWD}:/go/src/armada -w /go/src/armada armada-python-client-builder ./scripts/build-python-client.sh

airflow-operator:
	rm -rf proto-airflow
	mkdir -p proto-airflow

	docker buildx build -o type=docker $(dockerFlags) -t armada-airflow-operator-builder -f ./build/airflow-operator/Dockerfile .
	docker run --rm -v ${PWD}/proto-airflow:/proto-airflow -v ${PWD}:/go/src/armada -w /go/src/armada armada-airflow-operator-builder ./scripts/build-airflow-operator.sh

proto-setup:
	go run github.com/magefile/mage@v1.14.0 BootstrapProto

proto:
	go run github.com/magefile/mage@v1.14.0 proto

sql:
	$(GO_TEST_CMD) sqlc generate -f internal/scheduler/sql/sql.yaml
	$(GO_TEST_CMD) templify -e -p=sql internal/scheduler/sql/schema.sql

# Target for compiling the dotnet Armada REST client
dotnet: dotnet-setup proto-setup
	$(DOTNET_CMD) dotnet build ./client/DotNet/Armada.Client /t:NSwag
	$(DOTNET_CMD) dotnet build ./client/DotNet/ArmadaProject.Io.Client

# Pack and push dotnet clients to nuget. Requires RELEASE_TAG and NUGET_API_KEY env vars to be set
push-nuget: dotnet-setup proto-setup
	$(DOTNET_CMD) dotnet pack client/DotNet/Armada.Client/Armada.Client.csproj -c Release -p:PackageVersion=${RELEASE_TAG} -o ./bin/client/DotNet
	$(DOTNET_CMD) dotnet nuget push ./bin/client/DotNet/G-Research.Armada.Client.${RELEASE_TAG}.nupkg -k ${NUGET_API_KEY} -s https://api.nuget.org/v3/index.json
	$(DOTNET_CMD) dotnet pack client/DotNet/ArmadaProject.Io.Client/ArmadaProject.Io.Client.csproj -c Release -p:PackageVersion=${RELEASE_TAG} -o ./bin/client/DotNet
	$(DOTNET_CMD) dotnet nuget push ./bin/client/DotNet/ArmadaProject.Io.Client.${RELEASE_TAG}.nupkg -k ${NUGET_API_KEY} -s https://api.nuget.org/v3/index.json

# Download all dependencies and install tools listed in tools.yaml
download:
	go run github.com/magefile/mage@v1.14.0 BootstrapTools
	$(GO_TEST_CMD) go mod download
	$(GO_TEST_CMD) go mod tidy

generate:
	$(GO_CMD) go run github.com/rakyll/statik \
		-dest=internal/lookout/repository/schema/ -src=internal/lookout/repository/schema/ -include=\*.sql -ns=lookout/sql -Z -f -m && \
		go run golang.org/x/tools/cmd/goimports -w -local "github.com/armadaproject/armada" internal/lookout/repository/schema/statik

	go generate ./...

helm-docs:
	./scripts/helm-docs.sh

LOCALBIN ?= $(PWD)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

GOTESTSUM ?= $(LOCALBIN)/gotestsum

.PHONY: gotestsum
gotestsum: $(GOTESTSUM)## Download gotestsum locally if necessary.
$(GOTESTSUM): $(LOCALBIN)
	test -s $(LOCALBIN)/gotestsum || GOBIN=$(LOCALBIN) go install gotest.tools/gotestsum@v1.8.2

populate-lookout-test:
	if [ "$$(docker ps -q -f name=postgres)" ]; then \
		docker stop postgres; \
		docker rm postgres; \
	fi
	docker run -d --name=postgres $(DOCKER_NET) -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres:14.2
	sleep 5
	go test -v  ${PWD}/internal/lookout/db-gen/
