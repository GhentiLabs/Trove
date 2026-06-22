# Trove

Trove is a peer-to-peer backup and file-sync system: a user's folder is
replicated across a few trusted friends' machines, sending only changed data.
Peers connect **directly**, identified by an Ed25519 key — no certificate
authority, and **file data never passes through any server**.

This monorepo (one Go module, `github.com/GhentiLabs/Trove`) currently contains
the discovery server; shared code lives at the root under `pkg/`.

## Components

| Path | Status | What it is |
|------|--------|------------|
| [`discovery/`](discovery) | built | Always-on coordination service peers use to find each other and coordinate NAT hole punching. Not a relay. See its [README](discovery/README.md). |
| [`client/`](client) | planned | The peer daemon: file watching, chunking, the sync protocol, and direct encrypted peer connections. |
| [`pkg/identity`](pkg/identity), [`pkg/discovery`](pkg/discovery) | built | Shared Ed25519 identity + mTLS/pinning, and the discovery wire protocol. |

Component-private code lives under each component's `internal/`; only code shared
across components goes in root `pkg/`.

## Build & test

```sh
go build ./...
go test -race ./...
go vet ./...
golangci-lint run
```

Only the discovery server is implemented; the sync engine (chunking, hashing,
manifests, storage, encryption) is out of scope until the client is built.
