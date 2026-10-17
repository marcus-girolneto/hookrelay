# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/hookrelay ./cmd/hookrelay && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/receiver ./cmd/receiver

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hookrelay /out/receiver /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/hookrelay"]
