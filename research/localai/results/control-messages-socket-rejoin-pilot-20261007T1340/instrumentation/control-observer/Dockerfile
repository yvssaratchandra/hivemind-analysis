FROM golang:1.26-alpine AS tidy
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN go mod tidy

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN gofmt -w *.go && mkdir -p /out && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/control-observer .

FROM build AS test
RUN go test ./...

FROM scratch
COPY --from=build /out/control-observer /control-observer
ENTRYPOINT ["/control-observer"]
