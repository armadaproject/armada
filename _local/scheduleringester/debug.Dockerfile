FROM golang:1.24.5 AS builder
ENV GOTRACEBACK=all

WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY "cmd/scheduleringester/" "cmd/scheduleringester/"
COPY "internal/" "internal/"
COPY "pkg/" "pkg/"
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -gcflags="all=-N -l" -o scheduleringester "./cmd/scheduleringester"

FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates
RUN apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community delve

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

COPY --from=builder /app/scheduleringester /app/scheduleringester
COPY config/logging.yaml /app/config/logging.yaml
COPY config/scheduleringester/config.yaml /app/config/scheduleringester/config.yaml

WORKDIR /app
USER armada

ENTRYPOINT ["/app/scheduleringester"]
