ARG BASE_IMAGE=alpine:3.18.3.3

FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=loadtester
LABEL org.opencontainers.image.description="Load Tester"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/loadtester

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY armada-load-tester /app/

WORKDIR /app

ENTRYPOINT ["./armada-load-tester"]
