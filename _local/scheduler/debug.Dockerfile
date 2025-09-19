FROM golang:1.24.5 AS builder
ENV GOTRACEBACK=all

WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY "cmd/scheduler/" "cmd/scheduler/"
COPY "internal/" "internal/"
COPY "pkg/" "pkg/"
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -gcflags="all=-N -l" -o scheduler "./cmd/scheduler"

FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates
RUN apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community delve

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

COPY --from=builder /app/scheduler /app/scheduler
COPY config/logging.yaml /app/config/logging.yaml
COPY config/scheduler/config.yaml /app/config/scheduler/config.yaml

WORKDIR /app
USER armada

ENTRYPOINT ["/app/scheduler"]
CMD ["run"]
