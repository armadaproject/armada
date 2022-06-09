# Determine which platform we're on based on the kernel name
platform := $(shell uname -s || echo unknown)
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

GO_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada $(DOCKER_NET) \
	-e GOPROXY -e GOPRIVATE -e INTEGRATION_ENABLED=true -e CGO_ENABLED=0 -e GOOS=linux -e GARCH=amd64 \
	-v $(DOCKER_GOPATH_DIR):/go \
	golang:1.16-buster
DOTNET_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada \
	-v /etc/ssl/certs:/etc/ssl/certs \
	-e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
	mcr.microsoft.com/dotnet/sdk:3.1.417-buster
NODE_CMD = docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada/internal/lookout/ui \
	-e npm_config_disturl \
	-e npm_config_registry \
	-v /etc/ssl/certs:/etc/ssl/certs \
	-e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
	-e npm_config_cafile=/etc/ssl/certs/ca-certificates.crt \
	node:16.14-buster

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

build-lookout-ingester:
	$(GO_CMD) $(gobuild) -o ./bin/lookoutingester cmd/lookoutingester/main.go

build-eventapi-ingester:
	$(GO_CMD) $(gobuild) -o ./bin/eventingester cmd/eventapingester/main.go

build: build-server build-executor build-fakeexecutor build-armadactl build-load-tester build-binoculars build-lookout-ingester build-eventapi-ingester

build-docker-server:
	mkdir -p .build/server
	$(GO_CMD) $(gobuildlinux) -o ./.build/server/server cmd/armada/main.go
	cp -a ./config/armada ./.build/server/config
	docker build $(dockerFlags) -t armada -f ./build/armada/Dockerfile ./.build/server/

build-docker-executor:
	mkdir -p .build/executor
	$(GO_CMD) $(gobuildlinux) -o ./.build/executor/executor cmd/executor/main.go
	cp -a ./config/executor ./.build/executor/config
	docker build $(dockerFlags) -t armada-executor -f ./build/executor/Dockerfile ./.build/executor

build-docker-armada-load-tester:
	mkdir -p .build/armada-load-tester
	$(GO_CMD) $(gobuildlinux) -o ./.build/armada-load-tester/armada-load-tester cmd/armada-load-tester/main.go
	docker build $(dockerFlags) -t armada-load-tester -f ./build/armada-load-tester/Dockerfile ./.build/armada-load-tester

build-docker-armadactl:
	mkdir -p .build/armadactl
	$(GO_CMD) $(gobuildlinux) -o ./.build/armadactl/armadactl cmd/armadactl/main.go
	docker build $(dockerFlags) -t armadactl -f ./build/armadactl/Dockerfile ./.build/armadactl

build-docker-fakeexecutor:
	mkdir -p .build/fakeexecutor
	$(GO_CMD) $(gobuildlinux) -o ./.build/fakeexecutor/fakeexecutor cmd/fakeexecutor/main.go
	cp -a ./config/executor ./.build/fakeexecutor/config
	docker build $(dockerFlags) -t armada-fakeexecutor -f ./build/fakeexecutor/Dockerfile ./.build/fakeexecutor

build-docker-lookout-ingester:
	mkdir -p .build/lookoutingester
	$(GO_CMD) $(gobuildlinux) -o ./.build/lookoutingester/lookoutingester cmd/lookoutingester/main.go
	cp -a ./config/lookoutingester ./.build/lookoutingester/config
	docker build $(dockerFlags) -t armada-lookout-ingester -f ./build/lookoutingester/Dockerfile ./.build/lookoutingester

build-docker-eventapi-ingester:
	mkdir -p .build/eventingester
	$(GO_CMD) $(gobuildlinux) -o ./.build/eventingester/eventingester cmd/eventingester/main.go
	cp -a ./config/eventingester ./.build/eventingester/config
	docker build $(dockerFlags) -t armada-eventapi-ingester -f ./build/eventingester/Dockerfile ./.build/eventingester

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
	mkdir -p .build/binoculars
	$(GO_CMD) $(gobuildlinux) -o ./.build/binoculars/binoculars cmd/binoculars/main.go
	cp -a ./config/binoculars ./.build/binoculars/config
	docker build $(dockerFlags) -t armada-binoculars -f ./build/binoculars/Dockerfile ./.build/binoculars

build-docker: build-docker-server build-docker-executor build-docker-armadactl build-docker-armada-load-tester build-docker-fakeexecutor build-docker-lookout build-docker-lookout-ingester build-docker-binoculars

# Build target without lookout (to avoid needing to load npm packages from the Internet).
build-docker-no-lookout: build-docker-server build-docker-executor build-docker-armadactl build-docker-armada-load-tester build-docker-fakeexecutor build-docker-binoculars

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-armadactl-multiplatform build-load-tester

.ONESHELL:
tests-teardown:
	docker rm -f redis postgres || true

.ONESHELL:
tests-no-setup:
	$(GO_TEST_CMD) go test -v ./internal... 2>&1 | tee test_reports/internal.txt
	$(GO_TEST_CMD) go test -v ./pkg... 2>&1 | tee test_reports/pkg.txt
	$(GO_TEST_CMD) go test -v ./cmd... 2>&1 | tee test_reports/cmd.txt

.ONESHELL:
tests:
	mkdir -p test_reports
	docker run -d --name=redis $(DOCKER_NET) -p=6379:6379 redis:6.2.6
	docker run -d --name=postgres $(DOCKER_NET) -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres:14.2
	sleep 3
	function tearDown { docker rm -f redis postgres; }; trap tearDown EXIT
	$(GO_TEST_CMD) go test -v ./internal... 2>&1 | tee test_reports/internal.txt
	$(GO_TEST_CMD) go test -v ./pkg... 2>&1 | tee test_reports/pkg.txt
	$(GO_TEST_CMD) go test -v ./cmd... 2>&1 | tee test_reports/cmd.txt

# Rebuild and restart the server.
.ONESHELL:
rebuild-server: build-docker-server
	docker rm -f server || true
	docker run -d --name server --network=kind -p=50051:50051 -p 8080:8080 -v ${PWD}/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/nats/armada-config.yaml --config /e2e/setup/redis/armada-config.yaml --config /e2e/setup/pulsar/armada-config.yaml  --config /e2e/setup/server/armada-config.yaml

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
	docker rm -f nats redis pulsar server executor postgres || true
	kind delete cluster --name armada-test || true
	rm .kube/config || true
	rmdir .kube || true

.ONESHELL:
setup-cluster:
	kind create cluster --config e2e/setup/kind.yaml
	# We need an ingress controller to enable cluster ingress
	kubectl apply -f e2e/setup/ingress-nginx.yaml --context kind-armada-test
	# Wait until the ingress controller is ready
	echo "Waiting for ingress controller to become ready"
	sleep 60 # calling wait immediately can result in "no matching resources found"
	kubectl wait --namespace ingress-nginx \
		--for=condition=ready pod \
		--selector=app.kubernetes.io/component=controller \
		--timeout=90s \
		--context kind-armada-test
	docker pull "alpine:3.10" # ensure alpine, which is used by tests, is available
	docker pull "nginx:1.21.6" # ensure nginx, which is used by tests, is available
	kind load docker-image "alpine:3.10" --name armada-test # needed to make alpine available to kind
	kind load docker-image "nginx:1.21.6" --name armada-test # needed to make nginx available to kind
	mkdir -p .kube
	kind get kubeconfig --internal --name armada-test > .kube/config

tests-e2e-setup: setup-cluster
	docker run --rm -v ${PWD}:/go/src/armada -w /go/src/armada -e KUBECONFIG=/go/src/armada/.kube/config --network kind bitnami/kubectl:1.23 apply -f ./e2e/setup/namespace-with-anonymous-user.yaml

	# Armada dependencies.
	docker run -d --name pulsar -p 0.0.0.0:6650:6650 --network=kind apachepulsar/pulsar:2.9.2 bin/pulsar standalone
	docker run -d --name nats --network=kind nats-streaming:0.24.5
	docker run -d --name redis -p=6379:6379 --network=kind redis:6.2.6
	docker run -d --name postgres --network=kind -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres:14.2

	bash scripts/pulsar.sh

	sleep 30 # give dependencies time to start up
	docker run -d --name server --network=kind -p=50051:50051 -p 8080:8080 -v ${PWD}/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/nats/armada-config.yaml --config /e2e/setup/redis/armada-config.yaml --config /e2e/setup/pulsar/armada-config.yaml  --config /e2e/setup/server/armada-config.yaml
	docker run -d --name executor --network=kind -v ${PWD}/.kube:/.kube -v ${PWD}/e2e:/e2e  \
		-e KUBECONFIG=/.kube/config \
		-e ARMADA_KUBERNETES_IMPERSONATEUSERS=true \
		-e ARMADA_KUBERNETES_STUCKPODEXPIRY=15s \
		-e ARMADA_APICONNECTION_ARMADAURL="server:50051" \
		-e ARMADA_APICONNECTION_FORCENOTLS=true \
		armada-executor --config /e2e/setup/insecure-executor-config.yaml

.ONESHELL:
tests-e2e-no-setup:
	function printApplicationLogs {
		echo -e "\nexecutor logs:"
		docker logs executor
		echo -e "\nserver logs:"
		docker logs server
	}
	trap printApplicationLogs exit
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

python: download
	rm -rf proto
	mkdir -p proto
	mkdir -p proto/google/api
	mkdir -p proto/google/protobuf
	mkdir -p proto/k8s.io/apimachinery/pkg/api/resource
	mkdir -p proto/k8s.io/apimachinery/pkg/apis/meta/v1

	mkdir -p proto/k8s.io/apimachinery/pkg/runtime
	mkdir -p proto/k8s.io/apimachinery/pkg/runtime/schema
	mkdir -p proto/k8s.io/apimachinery/pkg/util/intstr/
	mkdir -p proto/k8s.io/api/networking/v1
	mkdir -p proto/k8s.io/api/core/v1
	mkdir -p proto/github.com/gogo/protobuf/gogoproto/

	docker build $(dockerFlags) -t armada-python-client-builder -f ./build/python-client/Dockerfile .
# Copy third party annotations from grpc-ecosystem
	
	cp $(DOCKER_GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway$(GRPC_GATEWAY_VERSION)/third_party/googleapis/google/api/annotations.proto proto/google/api
	cp $(DOCKER_GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway$(GRPC_GATEWAY_VERSION)/third_party/googleapis/google/api/http.proto proto/google/api
	cp $(DOCKER_GOPATH)/pkg/mod/github.com/gogo/protobuf$(GOGO_PROTOBUF_VERSION)/protobuf/google/protobuf/*.proto proto/google/protobuf
	cp -r $(DOCKER_GOPATH)/pkg/mod/github.com/gogo/protobuf$(GOGO_PROTOBUF_VERSION)/gogoproto/gogo.proto proto/github.com/gogo/protobuf/gogoproto/

#K8S MACHINERY API COPY
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/apimachinery$(K8_APIM_VERSION)/pkg/api/resource/generated.proto proto/k8s.io/apimachinery/pkg/api/resource/
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/apimachinery$(K8_APIM_VERSION)/pkg/apis/meta/v1/generated.proto proto/k8s.io/apimachinery/pkg/apis/meta/v1
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/apimachinery$(K8_APIM_VERSION)/pkg/runtime/generated.proto proto/k8s.io/apimachinery/pkg/runtime
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/apimachinery$(K8_APIM_VERSION)/pkg/runtime/schema/generated.proto proto/k8s.io/apimachinery/pkg/runtime/schema/
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/apimachinery$(K8_APIM_VERSION)/pkg/util/intstr/generated.proto proto/k8s.io/apimachinery/pkg/util/intstr/
#K8S API COPY
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/api$(K8_API_VERSION)/networking/v1/generated.proto proto/k8s.io/api/networking/v1
	cp $(DOCKER_GOPATH)/pkg/mod/k8s.io/api$(K8_API_VERSION)/core/v1/generated.proto proto/k8s.io/api/core/v1

	docker run --rm -v ${PWD}/proto:/proto -v ${PWD}:/go/src/armada -w /go/src/armada armada-python-client-builder ./scripts/build-python-client.sh

proto: download
	docker build $(dockerFlags) --build-arg GOPROXY --build-arg GOPRIVATE --build-arg MAVEN_URL -t armada-proto -f ./build/proto/Dockerfile .
	docker run --rm -e GOPROXY -e GOPRIVATE -v ${PWD}:/go/src/armada -w /go/src/armada armada-proto ./scripts/proto.sh

	# generate proper swagger types (we are using standard json serializer, GRPC gateway generates protobuf json, which is not compatible)
	$(GO_TEST_CMD) swagger generate spec -m -o pkg/api/api.swagger.definitions.json

	# combine swagger definitions
	$(GO_TEST_CMD) go run ./scripts/merge_swagger.go api.swagger.json > pkg/api/api.swagger.merged.json
	mv -f pkg/api/api.swagger.merged.json pkg/api/api.swagger.json

	$(GO_TEST_CMD) go run ./scripts/merge_swagger.go lookout/api.swagger.json > pkg/api/lookout/api.swagger.merged.json
	mv -f pkg/api/lookout/api.swagger.merged.json pkg/api/lookout/api.swagger.json

	$(GO_TEST_CMD) go run ./scripts/merge_swagger.go binoculars/api.swagger.json > pkg/api/binoculars/api.swagger.merged.json
	mv -f pkg/api/binoculars/api.swagger.merged.json pkg/api/binoculars/api.swagger.json

	rm -f pkg/api/api.swagger.definitions.json

	# embed swagger json into go binary
	$(GO_TEST_CMD) templify -e -p=api -f=SwaggerJson  pkg/api/api.swagger.json
	$(GO_TEST_CMD) templify -e -p=lookout -f=SwaggerJson  pkg/api/lookout/api.swagger.json
	$(GO_TEST_CMD) templify -e -p=binoculars -f=SwaggerJson  pkg/api/binoculars/api.swagger.json

	# fix all imports ordering
	$(GO_TEST_CMD) goimports -w -local "github.com/G-Research/armada" ./pkg/api/
	$(GO_TEST_CMD) goimports -w -local "github.com/G-Research/armada" ./pkg/armadaevents/

# Target for compiling the dotnet Armada client.
dotnet:
	$(DOTNET_CMD) dotnet build ./client/DotNet/Armada.Client /t:NSwag

# Download all dependencies and install tools listed in internal/tools/tools.go
download:
	$(GO_TEST_CMD) go mod download
	$(GO_TEST_CMD) go list -f '{{range .Imports}}{{.}} {{end}}' internal/tools/tools.go | xargs $(GO_TEST_CMD) go install
	$(GO_TEST_CMD) go mod tidy

code-reports:
	mkdir -p code_reports
	$(GO_TEST_CMD) goimports -d -local "github.com/G-Research/armada" . | tee code_reports/goimports.txt
	$(GO_TEST_CMD) ineffassign ./... | tee code_reports/ineffassign.txt

code-checks: code-reports
	sync # make sure everything has been synced to disc
	if [ $(shell cat code_reports/ineffassign.txt | wc -l) -ne "0" ]; then exit 1; fi
	if [ $(shell cat code_reports/goimports.txt | wc -l) -ne "0" ]; then exit 1; fi

generate:
	$(GO_CMD) go run github.com/rakyll/statik \
		-dest=internal/lookout/repository/schema/ -src=internal/lookout/repository/schema/ -include=\*.sql -ns=lookout/sql -Z -f -m && \
		go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" internal/lookout/repository/schema/statik
	$(GO_CMD) go run github.com/rakyll/statik \
    		-dest=internal/eventapi/eventdb/schema/ -src=internal/eventapi/eventdb/schema/ -include=\*.sql -ns=eventapi/sql -Z -f -m && \
    		go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" internal/eventapi/eventdb/schema/statik
