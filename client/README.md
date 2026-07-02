# client

The Trove peer daemon: it watches folders, chunks and hashes their contents,
and syncs them directly with the other members of each shared folder over
QUIC with mutual TLS pinning. The full design is in
[docs/client-blueprint.md](../docs/client-blueprint.md); milestone status and
deviations are recorded in its build-order section.

## Binaries

- `cmd/trove-peer` is the daemon and its management commands: `identity`,
  `found` (create a folder group), `invite`, `join`, `run` (the daemon),
  `restore` (recover a folder from any member or holder), `status`, `peers`,
  `quota`, and `remove`. A running daemon serves a control socket
  (`<state-dir>/control.sock`); the management commands use it when present,
  so changes take effect live, and fall back to the on-disk state otherwise.
- `cmd/trove-chunk` is a standalone chunking diagnostic, not part of the daemon.

## Layout

Package code lives under `internal/`, layered roughly as the blueprint
describes: content (`chunker`, `hasher`, `compression`, `crypto`,
`chunkstore`), model (`manifest`, `snapshot`, `model`), local state
(`scanner`, `watcher`, `storage`, `gc`), membership and wire
(`membership`, `wire`, `session`), transport and discovery (`transport`,
`discovery`, `peermgr`), the sync engine (`syncengine`), the untrusted
holder tier (`holder`), and the composition layer (`node`).

The client imports the shared root packages directly:

- [`pkg/identity`](../pkg/identity) — the same Ed25519 keypair and `node_id`
  derivation the discovery server uses.
- [`pkg/discovery`](../pkg/discovery) — the discovery wire protocol, so the
  client talks to the discovery server using identical types.

It does **not** import `discovery/internal/*` — those are private to the
discovery server (enforced by Go's `internal/` rule).

## Tests

`go test -race ./client/...` covers everything in-process. The end-to-end
NAT matrix in [`test/e2e`](test/e2e) runs the real binaries in Linux network
namespaces behind emulated NATs; see `matrix.sh`.
