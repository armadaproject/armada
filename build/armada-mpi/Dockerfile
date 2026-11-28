ARG BASE_IMAGE=alpine:3.21.3
FROM ${BASE_IMAGE}
LABEL org.opencontainers.image.title=armada-mpi
LABEL org.opencontainers.image.description="Starts MPI processes on the workers of an Armada MPI job without ssh"
LABEL org.opencontainers.image.url=https://hub.docker.com/r/gresearch/armada-mpi

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada
USER armada

COPY armada-mpi /app/

WORKDIR /app

ENTRYPOINT ["./armada-mpi"]
