FROM alpine:3.10

COPY ./executor /app/
COPY /config/executor/ /app/config/executor

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

WORKDIR /app

ENTRYPOINT ["./executor"]
