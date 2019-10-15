gobuildlinux = GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"
gobuild = go build

build-server:
	$(gobuild) -o ./bin/armada cmd/armada/main.go

build-executor:
	$(gobuild) -o ./bin/executor cmd/executor/main.go

build-armadactl:
	$(gobuild) -o ./bin/armadactl cmd/armadactl/main.go

build-load-tester:
	$(gobuild) -o ./bin/armada-load-tester cmd/armada-load-tester/main.go

build: build-server build-executor build-armadactl build-load-tester

build-docker-server:
	$(gobuildlinux) -o ./bin/linux/armada cmd/armada/main.go
	docker build -t armada -f ./build/armada/Dockerfile .

build-docker-executor:
	$(gobuildlinux) -o ./bin/linux/executor cmd/executor/main.go
	docker build -t armada-executor -f ./build/executor/Dockerfile .

build-docker: build-docker-server build-docker-executor

build-ci: gobuild=$(gobuildlinux)
build-ci: build-docker build-armadactl build-load-tester
