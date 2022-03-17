FROM alpine:3.10

RUN addgroup -S -g 2000 armada && adduser -S -u 1000 armada -G armada

USER armada

COPY ./bin/linux/fakeexecutor /app/

COPY /config/executor/ /app/config/executor

WORKDIR /app

ENTRYPOINT ["./fakeexecutor"]
