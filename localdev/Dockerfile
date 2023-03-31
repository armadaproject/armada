FROM golang:1.20-buster

RUN CGO_ENABLED=0 go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /

ENTRYPOINT ["bash"]
