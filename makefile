# Ruthlessly copied from containerd's Makefile:
# https://github.com/containerd/containerd/blob/57b51b94815aa2e592264eb69c2f98e3a6616adc/Makefile

ifneq "$(strip $(shell command -v go 2>/dev/null))" ""
	GOOS ?= $(shell go env GOOS)
	GOARCH ?= $(shell go env GOARCH)
else
	ifeq ($(GOOS),)
		# approximate GOOS for the platform if we don't have Go and GOOS isn't
		# set. We leave GOARCH unset, so that may need to be fixed.
		ifeq ($(OS),Windows_NT)
			GOOS = windows
		else
			UNAME_S := $(shell uname -s)
			ifeq ($(UNAME_S),Linux)
				GOOS = linux
			endif
			ifeq ($(UNAME_S),Darwin)
				GOOS = darwin
			endif
			ifeq ($(UNAME_S),FreeBSD)
				GOOS = freebsd
			endif
		endif
	else
		GOOS ?= $$GOOS
		GOARCH ?= $$GOARCH
	endif
endif


gobuild = GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build -ldflags="-s -w"

build-server:
	$(gobuild) -o armada cmd/armada/main.go
	docker build -t armada -f ./build/armada/Dockerfile .

build-executor:
	$(gobuild) -o executor cmd/executor/main.go
	docker build -t armada-executor -f ./build/executor/Dockerfile .

build-armadactl:
	$(gobuild) -o armadactl cmd/armadactl/main.go

build-load-tester:
	$(gobuild) -o armada-load-tester cmd/armada-load-tester/main.go

build: build-server build-executor build-armadactl build-load-tester
