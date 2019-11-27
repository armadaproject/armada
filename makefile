# use bash for running:
export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit

gobuildlinux = GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"
gobuild = go build

build-server:
	$(gobuild) -o ./bin/server cmd/armada/main.go

build-executor:
	$(gobuild) -o ./bin/executor cmd/executor/main.go

build-armadactl:
	$(gobuild) -o ./bin/armadactl cmd/armadactl/main.go

build-armadactl-multiplatform:
	go run github.com/mitchellh/gox -output="./bin/{{.OS}}-{{.Arch}}/armadactl" -arch="amd64" -os="windows linux darwin" ./cmd/armadactl/

ifndef RELEASE_VERSION
override RELEASE_VERSION = UNKNOWN_VERSION
endif

build-armadactl-release: build-armadactl-multiplatform
	mkdir ./dist || true
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-linux-amd64.tar.gz -C ./bin/linux-amd64/ armadactl
	tar -czvf ./dist/armadactl-$(RELEASE_VERSION)-darwin-amd64.tar.gz -C ./bin/darwin-amd64/ armadactl
	zip -j ./dist/armadactl-$(RELEASE_VERSION)-windows-amd64.zip ./bin/windows-amd64/armadactl.exe

build-load-tester:
	$(gobuild) -o ./bin/armada-load-tester cmd/armada-load-tester/main.go

build: build-server build-executor build-armadactl build-load-tester

build-docker-server:
	$(gobuildlinux) -o ./bin/linux/server cmd/armada/main.go
	docker build $(dockerFlags) -t armada -f ./build/armada/Dockerfile .

build-docker-executor:
	$(gobuildlinux) -o ./bin/linux/executor cmd/executor/main.go
	docker build $(dockerFlags) -t armada-executor -f ./build/executor/Dockerfile .

build-docker: build-docker-server build-docker-executor

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-load-tester

.ONESHELL:
tests:
	docker run -d --name=test-redis -p=6379:6379 redis
	function tearDown {
		docker stop test-redis
		docker rm test-redis
	}
	trap tearDown EXIT
	go test -v ./internal/...

e2e-start-cluster:
	./e2e/setup/setup_cluster_ci.sh
	./e2e/setup/setup_kube_config_ci.sh
	KUBECONFIG=.kube/config kubectl apply -f ./e2e/setup/namespace-with-anonymous-user.yaml

e2e-stop-cluster:
	docker stop kube
	docker rm kube

.ONESHELL:
tests-e2e: build-docker e2e-start-cluster
	docker run -d --name redis -p=6379:6379 redis
	docker run -d --name server --network=host -p=50051:50051 armada
	docker run -d --name executor --network=host -v $(shell pwd)/.kube/config:/kube/config \
		-e KUBECONFIG=/kube/config \
		-e ARMADA_KUBERNETES_IMPERSONATEUSERS=true \
		armada-executor
	function tearDown {
		echo -e "\nexecutor logs:"
		docker logs executor
		echo -e "\nserver logs:"
		docker logs server
		docker stop executor server redis
		docker rm executor server redis
	}
	trap tearDown EXIT
	echo -e "\nrunning test:"
	INTEGRATION_ENABLED=true go test -v ./e2e/test/... -count=1

