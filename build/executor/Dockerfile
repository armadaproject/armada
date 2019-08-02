FROM alpine:3.10

COPY ./executor /app/
COPY /config/executor/ /app/config/executor

WORKDIR /app

ENTRYPOINT ["./executor"]
