ARG BASE_IMAGE=alpine:3.21.3

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=scheduleringester
LABEL org.opencontainers.image.description="Scheduler Ingester"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/scheduleringester

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada
COPY scheduleringester /app/
COPY config/scheduleringester/config.yaml /app/config/scheduleringester/config.yaml
COPY config/logging.yaml /app/config/logging.yaml
WORKDIR /app
ENTRYPOINT ["./scheduleringester"]
