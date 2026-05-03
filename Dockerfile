# syntax=docker/dockerfile:1.7

FROM golang:1.26-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/grainfs ./cmd/grainfs

FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=build /out/grainfs /grainfs
VOLUME ["/data"]
EXPOSE 9000/tcp
EXPOSE 9002/tcp
EXPOSE 2049/tcp
EXPOSE 10809/tcp
EXPOSE 7001/udp
USER nonroot:nonroot
ENTRYPOINT ["/grainfs"]
CMD ["serve", "--data", "/data", "--port", "9000", "--nfs4-port", "0", "--nbd-port", "0"]
