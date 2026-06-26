# M5 frozen wire constants

The M5 bidirectional-sync changes to the M4 protocol. Frozen the moment M5 ships; a change
needs a `wire_format_version` bump. Golden-pinned in
`client/internal/wire/framing_test.go`. See also `docs/m4-wire-constants.md`,
`docs/m3-wire-constants.md`, and the codec rule in the m3-wire-codec-contract memory
(merely-transmitted fields → protobuf; signed/hashed → canonical hand-rolled — the new
fields below are all merely-transmitted).

## No new message types

M5 reuses the M4 control catalogue (`FolderSummary`=4, `ManifestRequest`=5,
`ManifestDelta`=6, `MembershipGossip`=7, `SyncReceipt`=8). Bidirectionality is a behavior
change — every node now sends and receives the whole catalogue, where M4 split it by
role — not a new message. The data-stream chunk protocol (`codec.go`) is unchanged.

## Field changes

`RemoteManifest`:
- **added** `author` (tag 12, string) — the node id that minted this version.
- **added** `authored_ms` (tag 13, int64) — that node's wall clock at the edit, in Unix
  milliseconds. With `author` it is the conflict-winner tiebreak input; it is **not**
  hashed into `manifest_id` or the snapshot leaf (the M2 frozen identity contract holds),
  it only travels alongside like the version vector.
- **removed** `owner_sequence` (was tag 8) — deleted, not reserved (pre-release, no
  installed base). Each node now assigns a fresh **local** sequence to every manifest it
  stores (originated or applied), so a per-manifest sender sequence is meaningless; the
  cursor advances on `ManifestDelta.high_water_sequence` (the sender's lineage high-water),
  not on a per-manifest field.

`FolderSummary`:
- **added** `sent_ms` (tag 5, int64) — the sender's wall clock at announce, for inter-node
  clock-skew **measurement** only (logged above a threshold). It never affects convergence
  and is ignored by reconciliation.

## Behavior contract (no wire change, but frozen semantics)

- **Cursor key is per-peer.** A node persists the consumed `(epoch, high_water)` per
  `(folder, peer)` in `peer_cursors` (renamed from M4's `replica_cursors`/`owner_peer_id`),
  and requests `since_sequence = high_water` from each peer. An `index_epoch_id` mismatch
  forces a full resync from that peer (`since = 0`).
- **Version vector is the global identity; the local sequence is delivery order only.** A
  received manifest is stored with its `version_vector` verbatim and a freshly allocated
  local seq, so it re-announces onward (gossip relay). The VV decides causality on apply
  (`dominates` / `concurrent`); the seq never does.
- **`SyncReceipt` is now bidirectional.** Both peers exchange it on convergence. A node
  records an incoming receipt as an `InboundAck` (a peer acked *my* lineage — the
  tombstone-reaping gate) and its own convergence as a `LocalSync` (last-synced reporting).
  The receipt body is unchanged; the direction is the receiver's local concern.

## Version vector on the wire

Unchanged from M4: canonical hand-rolled bytes (`manifest.VersionVector.Canonical`), never
protobuf — `uvarint(count)` then, in ascending node-id order, each entry as a
length-prefixed node id and a `uvarint` counter, zero counters omitted. Carried in
`RemoteManifest.version_vector` (tag 7) and re-parsed on apply.

## Conflict-copy path (not a wire field, but a frozen cross-node contract)

`ConflictPath(path, loserAuthor, loserAuthoredAt)` =
`<dir><stem>.conflict-<ts>-<loserNodeId><ext>`, where `<ts>` is the loser's `authored_ms`
formatted as `20060102T150405.000Z` in UTC. It is derived only from the loser's agreed
fields, so every node names the conflict copy identically. The author embedded here is
validated as a well-formed node id on apply, so it can never inject path segments.
