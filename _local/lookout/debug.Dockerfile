FROM openapitools/openapi-generator-cli:v5.4.0 AS openapi
COPY internal/lookoutui /project/internal/lookoutui
COPY pkg/api/*.swagger.json /project/pkg/api/
COPY pkg/api/binoculars/*.swagger.json /project/pkg/api/binoculars/
COPY pkg/api/schedulerobjects/*.swagger.json /project/pkg/api/schedulerobjects/
RUN ./project/internal/lookoutui/openapi.sh

FROM node:22.12-bullseye AS node
COPY --from=openapi /project/internal/lookoutui /lookoutui/
WORKDIR /lookoutui
RUN yarn install --immutable
RUN yarn build

FROM golang:1.24.5 AS builder
ENV GOTRACEBACK=all

WORKDIR /app
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY "cmd/lookout/" "cmd/lookout/"
COPY "internal/" "internal/"
COPY "pkg/" "pkg/"
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -gcflags="all=-N -l" -o lookout "./cmd/lookout"

FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates
RUN apk add --no-cache --repository=https://dl-cdn.alpinelinux.org/alpine/edge/community delve

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

COPY --from=node /lookoutui/build/ /app/internal/lookoutui/build
COPY --from=builder /app/lookout /app/lookout
COPY config/logging.yaml /app/config/logging.yaml
COPY config/lookout/config.yaml /app/config/lookout/config.yaml

WORKDIR /app
USER armada

ENTRYPOINT ["/app/lookout"]
