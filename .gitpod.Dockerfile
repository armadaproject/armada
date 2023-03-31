FROM golang:1.20.2-buster
USER root
RUN apt-get update \
    && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*
