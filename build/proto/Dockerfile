# Dockerfile for building .proto files.

ARG GOLANG_VERSION=1.16
ARG GOGO_PROTOBUF_VERSION=1.3.2
ARG GRPC_GATEWAY_VERSION=1.15.0
ARG GO_SWAGGER_VERSION=0.23.0
ARG TEMPLIFY_VERSION=0.0.0-20190823200653-c12e62ca00c1

# We download the protoc binary from Maven. Set this argument to use an alternate Maven repo.
ARG MAVEN_URL=https://repo1.maven.org/maven2

# We check the sha1 of the downloaded protoc binary, which must be added here manually, e.g., from
# https://repo1.maven.org/maven2/com/google/protobuf/protoc/
ARG PROTOC_VERSION=3.17.3
ARG PROTOC_SHA1=17c70df7057b054a221080a8178c421b6bfd4af0

# Populate with environment variables from shell by running docker build with
# --build-arg GOPROXY --build-arg GOPRIVATE
ARG GOPROXY=""
ARG GOPRIVATE=""

FROM --platform=linux/amd64 golang:${GOLANG_VERSION}-buster as builder

# Load GOPROXY and GOPRIVATE as environment varialbes in the build container.
# If not defined, these are set to "", which defaults to the standard go package source.
ARG GOPROXY
ARG GOPRIVATE
ENV GOPROXY=$GOPROXY
ENV GOPRIVATE=$GOPRIVATE

# Copy in the protoc toolchain (which must be downloaded and made available manually).
ARG PROTOC_VERSION
ARG PROTOC_SHA1
ARG MAVEN_URL
RUN curl -OLk ${MAVEN_URL}/com/google/protobuf/protoc/${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.exe && \
    mv protoc-${PROTOC_VERSION}-linux-x86_64.exe /usr/local/bin/protoc && \
    echo "${PROTOC_SHA1} /usr/local/bin/protoc" | sha1sum -c - && \
    chmod +x /usr/local/bin/protoc

ARG GOGO_PROTOBUF_VERSION
RUN GO111MODULE=on go get github.com/gogo/protobuf/protoc-gen-gogofaster@v${GOGO_PROTOBUF_VERSION}

ARG GRPC_GATEWAY_VERSION
RUN GO111MODULE=on go get \
   github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway@v${GRPC_GATEWAY_VERSION} \
   github.com/grpc-ecosystem/grpc-gateway/protoc-gen-swagger@v${GRPC_GATEWAY_VERSION}


ARG GOLANG_VERSION
FROM --platform=linux/amd64 golang:${GOLANG_VERSION}-buster
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /go/bin /usr/local/bin

ENTRYPOINT ["/bin/bash"]
