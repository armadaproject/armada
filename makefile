gobuild = GOOS=linux GARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"

build-server:
	$(gobuild) -o armada cmd/armada/main.go
	docker build -t armada -f ./build/armada/Dockerfile .

build-executor:
	$(gobuild) -o executor cmd/executor/main.go
	docker build -t armada-executor -f ./build/executor/Dockerfile .

build: build-server build-executor