# use bash for running:
export SHELL:=/bin/bash
export SHELLOPTS:=$(if $(SHELLOPTS),$(SHELLOPTS):)pipefail:errexit

gobuildlinux = GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"
gobuild = go build

build-server:
	$(gobuild) -o ./bin/server cmd/armada/main.go

build-executor:
	$(gobuild) -o ./bin/executor cmd/executor/main.go

build-fakeexecutor:
	$(gobuild) -o ./bin/executor cmd/fakeexecutor/main.go

build-armadactl:
	$(gobuild) -o ./bin/armadactl cmd/armadactl/main.go

build-binoculars:
	$(gobuild) -o ./bin/binoculars cmd/binoculars/main.go

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
	(cd ./internal/lookout/ui/ && npm install && npm run openapi && npm run build)
	$(gobuildlinux) -o ./bin/linux/lookout cmd/lookout/main.go
	docker build $(dockerFlags) -t armada-lookout -f ./build/lookout/Dockerfile .

build-docker-binoculars:
	$(gobuildlinux) -o ./bin/linux/binoculars cmd/binoculars/main.go
	docker build $(dockerFlags) -t armada-binoculars -f ./build/binoculars/Dockerfile .

build-docker: build-docker-server build-docker-executor build-docker-armadactl build-docker-armada-load-tester build-docker-fakeexecutor build-docker-lookout build-docker-binoculars

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-load-tester

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

e2e-start-cluster:
	./e2e/setup/setup_cluster_ci.sh
	./e2e/setup/setup_kube_config_ci.sh
	KUBECONFIG=.kube/config kubectl apply -f ./e2e/setup/namespace-with-anonymous-user.yaml
	#docker-compose -f ./e2e/setup/kafka/docker-compose.yaml up -d
	docker run -d --name nats -p 4223:4223 -p 8223:8223 nats-streaming -p 4223 -m 8223

e2e-stop-cluster:
	docker stop kube nats
	docker rm kube nats
	#docker-compose -f ./e2e/setup/kafka/docker-compose.yaml down

.ONESHELL:
tests-e2e: e2e-start-cluster build-docker
	docker run -d --name redis -p=6379:6379 redis
	docker run -d --name server --network=host \
		-v $(shell pwd)/e2e:/e2e \
		armada ./server --config /e2e/setup/insecure-armada-auth-config.yaml --config /e2e/setup/nats/armada-config.yaml
	docker run -d --name executor --network=host -v $(shell pwd)/.kube/config:/kube/config \
		-e KUBECONFIG=/kube/config \
		-e ARMADA_KUBERNETES_IMPERSONATEUSERS=true \
		-e ARMADA_KUBERNETES_STUCKPODEXPIRY=15s \
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
	export PATH=${PATH}:${PWD}/bin
	export INTEGRATION_ENABLED=true
	go test -v ./e2e/test/... -count=1

proto:
	docker build $(dockerFlags) -t armada-proto -f ./build/proto/Dockerfile .
	docker run -i --rm -v $(shell pwd):/go/src/armada -w /go/src/armada armada-proto ./scripts/proto.sh

generate:
	go run github.com/rakyll/statik \
		-dest=internal/lookout/repository/schema/ -src=internal/lookout/repository/schema/ -include=\*.sql -ns=lookout/sql -Z -f -m
	go run golang.org/x/tools/cmd/goimports -w -local "github.com/G-Research/armada" internal/lookout/repository/schema/statik
