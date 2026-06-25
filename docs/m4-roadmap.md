# M4 — One-Way Sync: roadmap

M4 makes the system real: one owner per folder originates snapshots; N replicas pull
and converge. It ships in phases so each is a working, testable artifact. Phase A is
implemented; B–D are preserved here with their acceptance gates.

Design constants that hold across all phases:
- Physical chunk backing only, owner and replica (~2× disk). The 1× storage
  optimization is deferred to M7; see `docs/m7-storage-optimization-design.md`.
- One-way semantics: owner send-only, replica receive-only; the conflict path is
  unreachable by construction.
- Wire is designed full: later phases add **new** message/stream types, never altering
  Phase A's frozen ones.

## Phase A — owner → one replica, single-source, crash-safe (DONE)

Sync wire messages (`FolderSummary`/`ManifestRequest`/`ManifestDelta`, golden-pinned),
the data-stream chunk protocol (`syncengine/codec.go`), reconciliation + anti-entropy
cursor, single-source puller with hash-verify, crash-safe apply (stage → fsync →
atomic rename → commit), and the model replica-apply path (`ApplyRemoteAndAdvance`,
version vectors stored verbatim, atomic with the cursor). Wired into the daemon via
`peermgr.OnSession` and `node` per-folder stores; `trove-peer -sync-role`.

**Accept (met):** a snapshot converges bit-exact to a replica (identical files, mode,
symlink targets, snapshot root, and leaf set); every chunk hash-verified; a rename
transfers no chunk data; a corrupt/truncated chunk is rejected and refetched; a failed
pull leaves no partial destination and an unadvanced cursor, then resumes; in-flight
chunks survive a grace-window sweep. Proven deterministically over MemNet + `synctest`
and real-QUIC loopback. M0–M3 tests still pass.

**Known Phase-A limits (addressed later):** `ManifestDelta` rides the 1 MiB
control-message cap, so a single full-resync delta is bounded; the owner refuses to
send an oversized delta (clear error, no reconnect loop) — large folders need
manifest-delta pagination (Phase C). Owner re-announce is a periodic ticker, not a
scanner push (acceptable anti-entropy latency). The replica has no startup
fs-reconcile, so out-of-band deletion of a synced file under the replica is not
re-materialized until the next delta (Phase E robustness). Manifest-serve goroutines
are unbounded per session (fine at one trusted peer/folder; bound in Phase C).
Tombstones keep their `manifest_chunks` rows after `SweepTombstones` removes the
manifest row (a pre-existing M2 metadata orphan, same on owner and replica); a
manifest-level GC pass is a later cleanup.

> **Storage optimization (1× disk) moved out of M4.** The original "virtual backing +
> `Promote`" Phase B is dissolved: a passive-scanner owner cannot Promote-before-overwrite
> (user edits land before the daemon sees them), so virtual backing is unsafe on the owner.
> The 1× work is reframed around reflink/CoW and deferred to M7 (retention), where history
> preservation becomes load-bearing. See `docs/m7-storage-optimization-design.md`. M4 ships
> physical-uniform storage throughout. Remaining M4 phases below are relabelled B–D.

## Phase B — multi-source scheduler

Design: `docs/m4-multisource-design.md`. **B1** (next): cross-(node,folder) scheduler
spanning sessions, swarm serving (any peer serves chunks it holds), spread + not-found
fallback to the owner, fastest-source-wins, in-flight dedup, bounded windows, and
manifest-delta pagination. **B2** (deferred): `Have`/bloom advertisements as a measured
optimization; optional rarest-first.

**Accept:** a fresh replica pulls distinct chunks from multiple peers in parallel; a
corrupt chunk from one source is rejected and transparently refetched from another;
memory stays bounded under a large folder and many peers.

## Phase C — membership (L4)

Network object + signed roster (`{node_id, role, added_by, sig, added_at}`) over
**canonical hand-rolled bytes, never protobuf**; gossip with last-seen; verify
signatures, reject and don't propagate bad ones; trusted-only.

**Accept:** a signed roster gossips to all members; a new member learns the full roster
from any one peer; an invalid-signature entry is rejected and not propagated.

## Phase D — SyncReceipts + deletion lifecycle + offline catch-up + live gate (built; live gate pending)

`sync_receipts` table exchanged on folder-sync completion; tombstone deletion applied
as a consistent snapshot unit (no resurrect on reconnect; reaped after convergence);
startup fs-reconcile; offline replica catches up via anti-entropy on reconnect.

Implemented: the `SyncReceipt` wire message (type 8) and `sync_receipts` table — a
replica records and reports convergence to its owner's root on each reconcile
completion; the owner stores one receipt per replica (`trove-peer status` prints them,
so "last synced" is queryable). `SweepTombstones` is gated on `safeSeq` = the minimum
high-water across replica receipts, so a deletion is reaped only after every known
replica has applied it (90-day retention is the backstop); an owner-side hourly sweeper
computes the gate. `RepairFolder` re-materializes a replica's out-of-band-deleted files
from local chunks at startup. The deletion-apply and atomic cursor commit were already
in place from Phase A. Covered by model units, engine-level convergence/repair/offline
catch-up tests, and the NAT matrix's live second round (edit + delete + rename + receipt
query over real holepunch).

**Accept (the M4 integration gate — human-run):** ≥3 real machines across NAT via
Trove/holepunch converge a multi-file folder through an edit, a delete, and a rename,
with one replica offline for part of the run; both ends hold correct SyncReceipts;
"last synced" is queryable. Procedure in `docs/m4-live-runbook.md`.
