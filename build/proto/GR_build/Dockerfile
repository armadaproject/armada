# Dockerfile for building .proto files internally at GR.

ARG GOLANG_VERSION=1.14
ARG PROTOC_VERSION=3.17.3
ARG GOGO_PROTOBUF_VERSION=1.3.2
ARG GRPC_GATEWAY_VERSION=1.15.0
ARG K8S_VERSION=0.17.3
ARG GO_SWAGGER_VERSION=0.23.0
ARG TEMPLIFY_VERSION=0.0.0-20190823200653-c12e62ca00c1
ARG GOIMPORTS_VERSION=0.0.0-20200529172331-a64b76657301

FROM --platform=linux/amd64 golang:${GOLANG_VERSION}-buster as builder

# Corporate proxy for go packages.
ENV GOPROXY=http://goproxy.uberit.net:3000
ENV GOPRIVATE=git.uberit.net

# Because we're building behind a corporate proxy,
# the protoc binaries have to be brought in manually via self-service downloads.
ARG PROTOC_VERSION
COPY build/proto/GR_build/protoc-${PROTOC_VERSION}-linux-x86_64/bin/protoc /usr/local/bin
COPY build/proto/GR_build/protoc-${PROTOC_VERSION}-linux-x86_64/include/* /proto
#RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip && \
#    unzip -o protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local bin/protoc && \
#    unzip -o protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local include/* && \
#    rm -rf protoc-${PROTOC_VERSION}-linux-x86_64.zip

ARG GOGO_PROTOBUF_VERSION
RUN GO111MODULE=on go get github.com/gogo/protobuf/protoc-gen-gogofaster@v${GOGO_PROTOBUF_VERSION} && \
    mkdir -p /proto/github.com/gogo/protobuf && \
    cp -r /go/pkg/mod/github.com/gogo/protobuf@v${GOGO_PROTOBUF_VERSION}/* /proto/github.com/gogo/protobuf && \
    mkdir -p /proto/google && \
    cp -r /go/pkg/mod/github.com/gogo/protobuf@v${GOGO_PROTOBUF_VERSION}/protobuf/google/* /proto/google

ARG GRPC_GATEWAY_VERSION
RUN GO111MODULE=on go get \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway@v${GRPC_GATEWAY_VERSION} \
    github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger@v${GRPC_GATEWAY_VERSION} && \
    mkdir -p /proto/google && \
    cp -r /go/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v${GRPC_GATEWAY_VERSION}/third_party/googleapis/google/* /proto/google

ARG K8S_VERSION
RUN go mod download k8s.io/api@v${K8S_VERSION} && \
    go mod download k8s.io/apimachinery@v${K8S_VERSION} && \
    mkdir -p /proto/k8s.io/api && \
    mkdir -p /proto/k8s.io/apimachinery && \
    cp -r $(go env GOPATH)/pkg/mod/k8s.io/api@v${K8S_VERSION}/* /proto/k8s.io/api && \
    cp -r $(go env GOPATH)/pkg/mod/k8s.io/apimachinery@v${K8S_VERSION}/* /proto/k8s.io/apimachinery
#RUN git clone --branch v${K8S_VERSION} https://github.com/kubernetes/api /proto/k8s.io/api && \
#    git clone --branch v${K8S_VERSION} https://github.com/kubernetes/apimachinery /proto/k8s.io/apimachinery

ARG GO_SWAGGER_VERSION
RUN GO111MODULE=on go get github.com/go-swagger/go-swagger/cmd/swagger@v${GO_SWAGGER_VERSION}

ARG TEMPLIFY_VERSION
RUN GO111MODULE=on go get github.com/wlbr/templify@v${TEMPLIFY_VERSION}

ARG GOIMPORTS_VERSION
RUN GO111MODULE=on go get golang.org/x/tools/cmd/goimports@v${GOIMPORTS_VERSION}

ARG GOLANG_VERSION
FROM --platform=linux/amd64 golang:${GOLANG_VERSION}-buster
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /go/bin /usr/local/bin
COPY --from=builder /proto /proto

ENTRYPOINT ["/bin/bash"]
