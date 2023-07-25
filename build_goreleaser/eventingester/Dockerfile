ARG BASE_IMAGE=alpine:3.18.0

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=eventingester
LABEL org.opencontainers.image.description="Event Ingester"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/eventingester

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada
COPY eventingester /app/
COPY config/eventingester/config.yaml /app/config/eventingester/config.yaml
WORKDIR /app
ENTRYPOINT ["./eventingester"]
