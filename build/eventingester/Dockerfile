ARG BASE_IMAGE=alpine:3.21.3

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=eventingester
LABEL org.opencontainers.image.description="Event Ingester"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/eventingester

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada
COPY eventingester /app/
COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml
COPY config/logging.yaml /app/config/logging.yaml

WORKDIR /app
ENTRYPOINT ["./eventingester"]
