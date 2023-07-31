ARG BASE_IMAGE=alpine:3.0
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=testsuite
LABEL org.opencontainers.image.description="Test Suite"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearchdev/testsuite

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY testsuite /app/

WORKDIR /app

ENTRYPOINT ["./testsuite"]
