ARG BASE_IMAGE=alpine:3.20.3

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=armada-server
LABEL org.opencontainers.image.description="Armada Server"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/armada-server

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY server /app/
COPY config/server/config.yaml /app/config/server/config.yaml

WORKDIR /app

ENTRYPOINT ["./server"]
