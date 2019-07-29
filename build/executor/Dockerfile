FROM golang:1.12.6

RUN mkdir /app

ADD . /app

WORKDIR /app

RUN ls

RUN go build -o main cmd/executor/main.go

CMD ["/app/main"]