gobuild = GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"

build-server:
	$(gobuild) -o armada cmd/armada/main.go
	docker build -t armada -f ./build/armada/Dockerfile .

build-executor:
	$(gobuild) -o executor cmd/executor/main.go
	docker build -t armada-executor -f ./build/executor/Dockerfile .

build-armadactl:
	$(gobuild) -o armadactl cmd/armadactl/main.go

build-load-test:
	$(gobuild) -o load-test cmd/load-test/main.go

build: build-server build-executor build-armadactl build-load-test
