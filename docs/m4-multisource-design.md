# M4 Phase B — multi-source transfer (B1): design

Phase A converges an owner to one replica, pulling chunks single-source from that one
session. Phase B lets a replica pull a folder's chunks from **several peers in
parallel** to aggregate bandwidth. In one-way mode there is one *owner*, so multiple
sources only help if **other replicas serve chunks they already hold** — a swarm. This
is consistent with one-way semantics: serving *chunks* is the content-addressed,
hash-verified data plane; only the owner ever originates *manifests/snapshots*.

**B1 scope (this doc):** cross-session scheduler, swarm serving, fastest-source-wins,
in-flight dedup, bounded windows, and manifest-delta pagination. **Deferred to B2:**
`Have`/bloom advertisements (a measured optimization on top of B1).

**Source model (decided):** *Have-free with not-found fallback.* A replica spreads its
wanted chunks across all connected folder-peers; a peer that lacks a chunk answers
`StatusNotFound`, and the replica falls back to the **owner**, which always holds every
chunk of the snapshot it announced. No `Have` message in B1.

## Architecture: per-session control, per-(node,folder) data

Today `Engine` is per-session and its puller pulls from that one session. B1 splits the
data plane out:

- **Per-session, unchanged in shape:** the control handler (announce/reconcile request
  + delta reply), the keepalive, and the **serve loop** (answers inbound chunk data
  streams). Serving is relaxed (below) so any peer serves chunks it holds.
- **Per-(node, folder) `coordinator`** (new, owned by `node.syncRuntime`): holds the
  set of **source** sessions for the folder and runs the multi-source scheduler. The
  replica's reconcile (still driven by the owner session's `FolderSummary`) computes the
  wanted chunk set per page and hands it to the coordinator, which spreads the pulls.

**Source-set lifecycle (no peermgr change):** when a session becomes Active, the engine
registers it with the coordinator of each shared folder; on session end it deregisters.
`peermgr` already invokes `OnSession`/stop at exactly these points. A source is marked
*owner* when a `FolderSummary` arrives from it (the announcer); the owner is the
guaranteed not-found fallback.

```
node.syncRuntime
 ├─ folder "demo" → *coordinator { sources: map[peerID]*source, owner: peerID, sched }
 └─ per session → *Engine { control handler, serve loop, registers self as a source }
```

## Swarm serving (relax the role check)

- `serveOneStream` currently rejects non-owner; change it to serve any chunk the node
  `Has` for a **shared** folder, regardless of role. A replica serving a chunk it holds
  originates nothing — the receiver hash-verifies it.
- The serve loop currently starts only when `ownsAny`; start it whenever the node has
  any folder (a replica must serve too). `serveSem` allocated likewise.
- One-way invariant is unchanged: only the owner sends `FolderSummary`/`ManifestDelta`;
  replicas still never originate manifests.

## Scheduler (coordinator.pull)

Input: the page's wanted chunk ids (already filtered by `missingChunks`). Maintains a
global in-flight window and a per-source window. For each chunk:

- pick the least-loaded source (round-robin among sources with free window); open a data
  stream, request the chunk;
- **not-found fallback:** `StatusNotFound`/error from a non-owner source → re-issue to
  the owner source;
- **fastest-source-wins:** a per-request stall timeout re-issues the chunk to another
  source; the first valid delivery wins, the loser is cancelled;
- **in-flight dedup:** a chunk is outstanding to one source at a time (until a stall);
- verify by re-hash before store (Phase A's `fetchOne`, now source-parameterised);
- bounded windows cap memory regardless of folder size or source count.

`fetchOne` becomes `fetch(source, id)` — the only change is *which* conn it opens the
stream on. The owner always being a valid source guarantees progress even if every
peer-replica lacks a chunk.

## Manifest-delta pagination (folds in the Phase A 1 MiB cap)

Multi-source implies larger folders, so finish the deferred pagination. Add `bool
complete` to `ManifestDelta`. The owner's `buildDelta` returns manifests since the
cursor up to a byte budget (≤ `MaxControlMessageSize` with headroom) and sets
`complete=false` if more remain. The replica processes **page by page**, each page a
full Phase-A apply unit (pull its chunks multi-source → stage → fsync → commit cursor to
the page's high-water), then requests the next page (`since = page high-water`) until
`complete`. This keeps crash-safety and bounds memory to one page; convergence is the
last (complete) page. No new message — one field plus a request loop.

## model / GC

Unchanged in shape. `model` stays the GC authority; a replica serving chunks does not
change reachability. Per-page apply keeps the Phase-A ordering (chunks stored before the
manifest that references them; grace protects in-flight).

## Test plan (deterministic, matches Phase A conventions)

- **3-peer convergence:** owner + replica-1 converge; then a fresh replica-2 joins with
  both owner and replica-1 as sources. Assert replica-2 converges bit-exact and that
  **distinct chunks were served by both** owner and replica-1 (per-source served-chunk
  counters), proving parallel multi-source — not all from the owner.
- **Corrupt/missing source:** one source serves a corrupt chunk (existing `corruptStream`)
  or `StatusNotFound`; assert the chunk is rejected and transparently refetched from
  another source, and convergence still bit-exact.
- **Pagination:** a folder whose full delta exceeds the control cap converges across
  multiple pages; assert > 1 page and bounded peak memory.
- **Backpressure:** many sources + large folder; assert outstanding requests stay within
  the configured windows.
- **No regression:** all Phase A tests pass; real-QUIC loopback still converges; the
  one-way invariants (replica never announces; only owner originates) still hold.

## Risks / edge cases

- A source disconnecting mid-pull: deregister; in-flight chunks to it time out and
  re-issue to another source (owner fallback guarantees completion).
- Owner advancing during a paged sync: a later page reflects the newer high-water; the
  next announce reconciles any tail (Phase A behaviour, per page).
- Duplicate delivery after a stall re-issue: first valid store wins; the late one is a
  verified no-op (same chunk id) — harmless.
- Self-as-source: never register the local node; only remote sessions are sources.
