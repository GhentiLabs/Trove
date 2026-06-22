# Trove discovery server

A small, always-on coordination service that lets Trove peers find one another
and coordinate NAT hole punching. It is one component of the
[Trove project](../README.md) — **not** the peer client or the sync engine.

It is deliberately lean enough to run on a single GCP Always Free e2-micro VM
(1 shared vCPU, 1 GB RAM), and it is **not a relay**: it only forwards small JSON
control messages (capped at 4 KB) and never proxies or stores peer file data.

## What it does

1. **Announce** — a peer publishes its current candidate addresses (a phone book).
2. **Lookup** — a peer resolves another peer's addresses by node ID.
3. **Signal** — over WebSocket, it brokers a hole-punch: each side gets the
   other's candidates plus a synchronized "punch" time.
4. **Analytics** — peers post telemetry, stored durably in a separate SQLite DB.
5. **Health & metrics** — a Prometheus endpoint and a `/healthz` probe, both on a
   private localhost-only listener.

## Identity & mTLS

Each node owns one Ed25519 keypair — the single, stable identity that also
authenticates its direct peer-to-peer connections. A node ID is the fingerprint
of the key's SubjectPublicKeyInfo, derived through the shared
[`pkg/identity`](../pkg/identity) package so server, clients, and peers all
compute it identically:

```
node_id = lowercase(base32_nopad(sha256(SubjectPublicKeyInfo)))   # 52 characters
```

The server terminates **TLS 1.3** itself with a persistent self-signed
certificate (the cert is regenerable; the key is the identity). There is **no
certificate authority and no domain**: clients trust the server by **pinning its
fingerprint**, and the server authenticates clients by **mutual TLS** —
`RequireAnyClientCert` with no CA check, deriving each caller's `node_id` from the
presented certificate. mTLS provides authentication, integrity, and replay
protection at the connection layer, so requests carry no signatures.

On boot the server logs its fingerprint and a `trove://<host>:<port>?id=<fingerprint>`
connection string for the operator to distribute.

## API

All endpoints require an mTLS client certificate. Bodies are JSON; errors use one
envelope: `{"code": "...", "message": "..."}` with generic messages (detail is
logged server-side only). The caller's `node_id` always comes from the client
certificate, never the body.

- `POST /v1/announce` — publish candidate addresses; returns granted TTL, expiry,
  and the server-observed source address.
- `POST /v1/lookup` — resolve a target `node_id`; `200` with
  `{node_id, addresses, last_seen_millis}` or `404`.
- `POST /v1/analytics` — store an open-ended `fields` map plus `install_id`,
  `schema_version`, and an event timestamp. Backed by its own SQLite DB, its own
  rate limiter, and a disk-usage cap; returns `507` when the cap is reached rather
  than filling the disk.
- `GET /v1/signal` (WebSocket) — client opens with a `hello`; then
  `connect_request{target_node_id, my_candidates[]}` yields `incoming_request`
  to the target and `peer_candidates` to the requester (with a near-future
  `punch_at_millis`), or `target_unavailable` if the target has no live
  connection.

### Metrics & health (private)

`GET /metrics` (Prometheus) and `GET /healthz` are served on a **separate,
localhost-only** listener (default `127.0.0.1:9090`), never on the public mTLS
port and never through the firewall — reach them via SSH tunnel. Metric names are
prefixed `trove_discovery_` so other Trove components never collide. Counters are
labelled by endpoint and outcome; per-source detail lives in the structured logs.

## Configuration

Environment variables (prefix `TROVE_DISCOVERY_`), overridable by flags. Selected
settings:

| Env | Flag | Default | Meaning |
|-----|------|---------|---------|
| `TROVE_DISCOVERY_LISTEN_ADDR` | `-listen-addr` | `0.0.0.0:8443` | public mTLS listener |
| `TROVE_DISCOVERY_METRICS_ADDR` | `-metrics-addr` | `127.0.0.1:9090` | localhost-only metrics + health listener |
| `TROVE_DISCOVERY_SERVER_KEY` | `-server-key` | `server.key` | persistent Ed25519 key (identity) |
| `TROVE_DISCOVERY_SERVER_CERT` | `-server-cert` | `server.crt` | self-signed certificate |
| `TROVE_DISCOVERY_REQUIRE_CLIENT_CERT` | `-require-client-cert` | `true` | demand an mTLS client cert |
| `TROVE_DISCOVERY_ADVERTISE_ADDR` | `-advertise-addr` | (none) | public host or host:port for the `trove://` string |
| `TROVE_DISCOVERY_TTL_MIN/DEFAULT/MAX` | `-ttl-*` | `1m`/`10m`/`1h` | announce TTL bounds (clamped) |
| `TROVE_DISCOVERY_MAX_SIGNAL_BYTES` | — | `4096` | per signaling-message cap |
| `TROVE_DISCOVERY_MAX_BODY_BYTES` | — | `16384` | announce/lookup body cap |
| `TROVE_DISCOVERY_ANALYTICS_MAX_BODY_BYTES` | — | `262144` | analytics body cap |
| `TROVE_DISCOVERY_REGISTRY_MAX_ENTRIES` | — | `100000` | registry entry cap |
| `TROVE_DISCOVERY_ANALYTICS_DB` | `-analytics-db` | `analytics.db` | analytics SQLite path |
| `TROVE_DISCOVERY_ANALYTICS_DISK_CAP_BYTES` | `-analytics-disk-cap` | `268435456` | stop ingest above this |
| `TROVE_DISCOVERY_MAX_WS_CONNS` | `-max-ws-conns` | `5000` | concurrent signaling cap |
| `TROVE_DISCOVERY_WS_ALLOWED_ORIGINS` | — | (none) | allowed WebSocket origins |
| `TROVE_DISCOVERY_RATE_{ANNOUNCE,LOOKUP,ANALYTICS,SIGNAL}_{RPS,BURST}` | — | see `internal/config` | per-IP/per-node limits |

See [`internal/config`](internal/config) for the complete list. `-healthcheck`
probes the local `/healthz` and exits (used by the container healthcheck).

## Running locally

From the repository root:

```sh
make run            # go run ./discovery/cmd/discovery-server, mTLS on 0.0.0.0:8443
                    # creates server.key/server.crt and logs the fingerprint
curl http://127.0.0.1:9090/healthz   # health/metrics are on the loopback listener

make test           # go test ./...
make race           # go test -race ./...
make vet lint       # go vet + golangci-lint
make build          # static binary at bin/discovery-server
```

A pinned mTLS client (using [`pkg/identity`](../pkg/identity)'s `PinnedClientConfig`
/ `DialPinned`) is required to reach the public endpoints.

## Deployment

The server terminates TLS itself — there is no reverse proxy. See [`deploy/`](deploy):

```sh
cd discovery/deploy
docker compose up -d --build
docker compose logs discovery-server | grep fingerprint   # distribute trove://…?id=<fp>
```

Full free-tier walkthrough: [`deploy/GCP_SETUP.md`](deploy/GCP_SETUP.md).

## Layout

```
discovery/
  cmd/discovery-server   entrypoint, key/cert load, TLS server, graceful shutdown
  internal/config        env + flag configuration       (private to this component)
  internal/registry      in-memory TTL phone book (bounded)
  internal/signaling     WebSocket hole-punch broker
  internal/analytics     SQLite telemetry store with disk cap
  internal/httpapi       routing, middleware, mTLS identity, metrics
  deploy                 Dockerfile, docker-compose.yml, GCP guide
```

`internal/*` is private to the discovery server (enforced by Go's `internal/`
rule); shared code lives in the repo-root [`pkg/`](../pkg).

## Extending: abuse handling

The code ships the extension points for spam/abuse handling but **no detection or
blocking logic** — by design:

- a no-op `Filter` consulted on every request ([`internal/httpapi/abuse.go`](internal/httpapi/abuse.go)),
- an empty `Denylist` (by IP and by node ID) consulted on every request,
- per-source structured logs and per-endpoint counters that already capture the
  signal a future detector would need.

These are the single, clean seams to add real policy later without restructuring.
