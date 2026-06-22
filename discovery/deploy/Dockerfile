# Build a static, CGO-free binary (modernc.org/sqlite is pure Go) and ship it on
# a minimal non-root distroless image. Build context is the repository root.
FROM golang:1.26.4 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /discovery-server ./discovery/cmd/discovery-server

# Empty data dir whose ownership a named volume inherits on first mount.
RUN mkdir -p /data

FROM gcr.io/distroless/static:nonroot@sha256:963fa6c544fe5ce420f1f54fb88b6fb01479f054c8056d0f74cc2c6000df5240
COPY --from=build /discovery-server /discovery-server
COPY --from=build --chown=nonroot:nonroot /data /data

EXPOSE 8443
USER nonroot:nonroot
ENTRYPOINT ["/discovery-server"]
