FROM alpine:3.18.2

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./fakeexecutor /app/

COPY /config/ /app/config/executor

WORKDIR /app

ENTRYPOINT ["./fakeexecutor"]
