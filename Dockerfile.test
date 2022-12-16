# syntax=docker/dockerfile:1

FROM golang:1.19

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY *.go ./
COPY vendor ./vendor

CMD go test -v -race -tags=integration -parallel 1 ./... -coverprofile=coverage.out
