ARG BASE_IMAGE=alpine:3.0
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=fakeexecutor
LABEL org.opencontainers.image.description="Fake Executor"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/armada-fakeexecutor

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY fakeexecutor /app/
COPY config/executor/config.yaml /app/config/executor/config.yaml

WORKDIR /app

ENTRYPOINT ["./fakeexecutor"]
