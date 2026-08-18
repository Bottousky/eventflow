# syntax=docker/dockerfile:1.7
#
# EventFlow Dockerfile. The image is a small multi-stage Go build that
# ends with a distroless base. EventFlow does not require Docker to
# run; this file is provided so the project can be exercised in any
# environment that has Docker installed.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

# Build the static binary. CGO is disabled to keep the final image
# small and to make the binary portable across libc variants; the
# modernc.org/sqlite driver is pure-Go.
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags="-s -w" -o /out/eventflow ./cmd/eventflow

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/eventflow /app/eventflow
USER nonroot:nonroot
ENV DB_PATH=/data/eventflow.db \
    HTTP_ADDR=:8080
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/app/eventflow"]
