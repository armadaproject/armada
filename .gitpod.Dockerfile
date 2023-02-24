FROM golang:1.18.5-buster
USER root
RUN apt-get update \
    && apt-get install -y \
    && rm -rf /var/lib/apt/lists/*
