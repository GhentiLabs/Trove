# client (placeholder)

The Trove peer daemon — file watching, chunking, hashing, the sync protocol, and
direct encrypted peer connections — is **not built yet**. This directory is a
reserved placeholder for that component.

When implemented, the client will import the shared root packages directly:

- [`pkg/identity`](../pkg/identity) — the same Ed25519 keypair and `node_id`
  derivation the discovery server uses.
- [`pkg/discovery`](../pkg/discovery) — the discovery wire protocol, so the
  client talks to the discovery server using identical types.

It will **not** import `discovery/internal/*` — those are private to the
discovery server (enforced by Go's `internal/` rule). See the repository
[README](../README.md) for the overall design.
