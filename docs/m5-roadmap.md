# M5 — Bidirectional Sync: roadmap

M5 turns the one-way folder into a two-way one: every `writer` originates edits and
bumps its own version-vector counter, every node serves and pulls, and concurrent edits
to the same path resolve — with no coordination — to a deterministic keep-both outcome
that converges byte-identically on every node. It ships in phases; each is a working,
reviewable, tested artifact. Built on branch `m5-bidirectional-sync`.

The conflict machinery is **new**, not a dormant flag: M4 shipped pure one-way (replicas
applied the owner's version vectors verbatim and never originated), so concurrent-VV
detection and keep-both resolution did not exist. M5 builds them (M4 deviation #2).

Design constants that hold across all phases:
- **Convergence with no coordination.** Every piece of resolution is a pure function over
  data every node already agrees on: the two concurrent manifests' version vectors, the
  authoring `node_id`s, and an author edit-timestamp carried in the manifest. The unique
  `node_id` is the final total-order tiebreak, so determinism never depends on the clock.
- **Plain version vector per path, one entry per node, never pruned.** Each node is a
  single serialized writer behind its own counter — the case where a plain VV is exact;
  Dotted Version Vectors and clock pruning are unnecessary (they fix many-writers-per-entry
  multiplexing, which does not occur here).
- **The author edit-timestamp is a sidecar**, never hashed into the manifest id or the
  snapshot leaf (the M2 frozen identity contract is untouched), so history is unaffected.
- **Trusted-only**, physical chunk backing (carried from M4). Trust/encryption modes are
  M6; 1× reflink storage is M7.

## Phase 0 — model primitives (DONE)

Version-vector comparison (`Compare`/`Dominates`/`IsConcurrent`/`Join`); the author
edit-timestamp sidecar on `model.Record` + a `manifests` column + the `RemoteManifest`
wire field (golden round-trip); the pure `ConflictWinner(authoredAt, nodeID)` total order.

**Accept (met):** winner function total, deterministic, order-independent, and
node_id-decided under equal/garbage timestamps; `Join` is commutative/associative/
idempotent; existing manifest identity and snapshot roots are byte-identical (the sidecar
changed no hash). Schema bumped to v2.

## Phase 1 — writers originate + two-way anti-entropy (DONE)

`model.ApplyRemote` resolves each incoming manifest against the local version — fast
forward (incoming dominates), ignore (local dominates), no-op (equal), or merge on
concurrent-but-identical content — and assigns a **fresh local sequence number** so an
applied manifest is re-served onward (gossip relay). The cursor is per-`(folder, peer)`.
`applyMu` serializes apply against local origination. The engine became role-symmetric:
every node announces, serves manifests and chunks, and pulls; `Role` is `Writer`
(originates) or `Reader` (relays only). `sync_receipts` split by direction
(`InboundAck` drives the reaping gate, `LocalSync` drives last-synced reporting), since
both directions now exist per peer. `folderRole` derives the tier from the membership
roster.

**Accept (met):** two writers with non-overlapping offline edits converge bit-exact; a
writer's edit relays transitively A→B→C with no direct A–C link; one-way (owner+readers)
folders are unchanged. Proven over MemNet.

## Phase 2 — conflict detection + keep-both resolution (DONE)

On concurrent + different content, the deterministic winner keeps the path with the
**joined** vector (so it dominates the loser everywhere and re-detection is idempotent);
the loser is preserved as a conflict copy at `ConflictPath(path, loserAuthor,
loserAuthoredAt)` carrying the loser's content and vector **verbatim** — so
`{copy-path, loser-content, loser-VV}` is byte-identical on every node and is a fixpoint,
never a fresh edit that could spawn conflicts forever. Identical-content concurrency
merges vectors with no copy.

**Accept (met):** two writers editing the same path converge to byte-identical
`{winner, conflict copy}`; resolution is order-independent and idempotent; a 3-node
writer+writer+reader set converges identically (the reader resolves the same way); no
conflict-copy-of-conflict-copy.

## Phase 3 — delete-vs-edit unified, two-way tombstones (DONE)

A tombstone is a VV-versioned event, so delete-vs-edit is one mechanism: the **edit wins**
the path (data is never lost), identical-content-same-state merges vectors, and two
concurrent deletes converge to a single deterministic tombstone. This also fixed a
divergence where a live file and a tombstone of the same content (equal manifest id,
different deleted flag) merged to opposite outcomes per node. Tombstone reaping is
membership-aware: `ConvergedHighWater` gates on every roster member having acked, so a
long-partitioned member cannot resurrect a reaped delete.

**Accept (met):** concurrent A-deletes / B-edits keeps the edit on both; delete-vs-delete
converges to deleted; the M4 dominated-delete catch-up (no resurrection of a delete the
peer has seen) still holds.

## Phase 4 — cutover, hardening, push, skew, E2E (DONE; live NAT cell pending human run)

- **Cutover gate (the real risk):** an existing one-way folder with retained history,
  with a replica promoted to writer, both editing the same path in one offline window:
  on reconnect they converge to byte-identical conflict copies **and** the pre-existing
  snapshot still materializes bit-exact (the model change does not corrupt history).
- **Push-on-change** (blueprint L8.4): a committed model change notifies every session for
  the folder to re-announce immediately, collapsing relay/edit latency from the 5s tick to
  sub-second.
- **Clock-skew measurement:** `FolderSummary.sent_ms` lets the receiver log inter-node
  skew above a threshold, to decide empirically whether an HLC is ever warranted. Skew
  never affects convergence (the node_id decides ties).
- **Hardening from review:** malformed-author rejection (conflict-path injection),
  empty-apply fast path (no stall on a transient FS error), dotfile conflict naming.

**Accept (met in-process):** the cutover gate; an extensive multi-node E2E simulation —
random-op mesh convergence with a fixpoint re-scan, partition/heal rounds, a conflict
storm that preserves every edit, and concurrent edits over a corrupting link — all green
under `-race`.

**Remaining for M5-done: the live bidirectional NAT-matrix cell** (two writers, concurrent
offline edits to one path over real holepunch, converge to identical conflict copies) and
the human ≥2-machine confirmation, mirroring M4's `offline-gate`.

## Deviations from the blueprint (deliberate, reviewed)

1. **Push is implemented now** (blueprint L8.4) rather than deferred, because two-way
   relay latency made the 5s-tick-only model visibly slow; it is a clean model-change hook
   fanning out through the per-folder coordinator.
2. **Tombstone reaping is membership-aware** (gated on every roster member, not just peers
   seen) — stronger than M4's seen-peers gate, closing a mesh zombie-resurrection gap.
3. **Delete-vs-edit is edit-wins in place** (not resurrect-as-conflict-copy): the edit
   stays at the real path and the delete is dropped, which preserves data without an extra
   copy and is the simpler deterministic rule.

## Deferred (NOT M5)

Trust/encryption + untrusted (`holder`) peers and convergent vs random keys → M6. 1×
reflink/CoW storage → M7. Member revocation (the owner is the anchor) — until it exists, a
permanently-offline member blocks reaping of *new* tombstones past convergence (bounded by
the number of deletes; the retention window remains a co-requirement). HLC for the conflict
timestamp — only if measured skew shows wrong-winner picks. LWW as an opt-in policy tail on
the same winner function.
