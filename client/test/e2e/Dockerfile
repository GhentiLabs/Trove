# NAT hole-punch matrix harness. Builds the discovery server and trove-peer, then
# (at run time, with --privileged) constructs a Linux netns topology that puts two
# peers behind separate emulated NATs and a coordinator on a shared "internet"
# segment. Because Linux netfilter is a real NAT, the punch outcomes mirror the
# real world for the NAT classes it expresses. See run.sh.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -o /out/discovery-server ./discovery/cmd/discovery-server \
 && go build -trimpath -o /out/trove-peer ./client/cmd/trove-peer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      iproute2 nftables iptables conntrack ca-certificates procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/discovery-server /out/trove-peer /usr/local/bin/
COPY client/test/e2e/run.sh /usr/local/bin/run.sh
RUN chmod +x /usr/local/bin/run.sh
ENTRYPOINT ["/usr/local/bin/run.sh"]
