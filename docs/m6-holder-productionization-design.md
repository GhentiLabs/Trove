# M6 holder productionization — design

Status: approved. Restore authorization = **Model B (verifier-proven, read-only)**, decided
2026-06-27. Branch `m6-trust-encryption`.

This makes the untrusted **holder** tier production-grade. Today a writer re-uploads the
whole folder to a holder on every connect, serves only a connect-time snapshot, pushes blobs
one-at-a-time, has no restore command, and is proven only over an in-memory network. This
design fixes all of that. Vocabulary: a *holder* is an untrusted node that stores a folder as
opaque, key-blinded ciphertext blobs it can never open; a *writer* holds the key.

Four pieces, in dependency order: (1) incremental & live push, (2) parallel push, (3) explicit
restore command, (4) real-network holder test. (1) and (2) share new wire ops, so they ship
together; (3) depends on a decision below; (4) depends on (3).

---

## 0. The enabling change: content-addressed catalog + a tiny pointer

Today the catalog (the sealed list of all manifests, the restore index) lives at one **fixed**
blinded id and is re-sealed (`SealMutable`, random nonce) and re-pushed every time. Because its
id is fixed, a holder cannot be asked "do you already have the current catalog?" — the answer is
always "I have *a* catalog," not "I have *this* one." That blocks skipping an unchanged catalog.

Change the layout to two blobs:

- **Catalog blob** — id = `BLAKE3(catalogBytes)`, stored at `blind(catalogID)`, sealed
  **convergently** (`Seal(master, catalogID, bytes)`). Safe from the nonce-reuse bug that bit us
  earlier *because* the id is the content hash: identical content → identical (key, nonce) sealing
  identical bytes; different content → different id → different nonce. Each tree version is a new,
  content-addressed, dedup-able, **HAS-skippable** blob.
- **Pointer blob** — fixed blinded id `blind("catalog-pointer/v1")`, content = the current
  catalog id (32 bytes), sealed with `SealMutable` (random nonce, since a fixed id with changing
  content is the one place that needs it). Tiny; always pushed last; it is the atomic commit.

Restore becomes: fetch+open the pointer → learn the current catalog id → fetch+open that catalog
→ fetch+open chunks. One extra round-trip, paid once per restore.

This is the layout the security reviewer originally suggested; it is correct here precisely
because the pointer uses the random-nonce seal.

---

## 1. Incremental & live push

### 1a. Stateless reconciliation (no durable per-holder bookkeeping)

The **holder's actual contents are the source of truth.** On a push the writer:

1. Builds the current live manifest set → catalog bytes → `catalogID`.
2. Forms the *needed set* of blinded ids: `blind(catalogID)` plus `blind(chunkID)` for every
   unique chunk the live tree references.
3. **Batch-asks** the holder which it already has (`opHasBatch`, below), in batches of ≤4096.
4. Pushes only the absent ones — chunks first (in parallel, §2), then the catalog blob.
5. Pushes the pointer last (always; it is tiny) → commits the new version.

No durable writer-side state is required for correctness. A 40,000-chunk folder reconciles in
~10 batched round-trips; a no-change reconnect is ~10 round-trips + one tiny pointer write
(the catalog is HAS-present, so skipped). First connect is a full push.

> **Example.** 50 GB photo library already backed up. WiFi reconnects: the writer batch-asks
> "have these 40,000 chunks + this catalog?", the holder says "yes to all," the writer pushes
> only the 32-byte pointer. Edit 3 photos: only those 3 files' new chunks + the new catalog blob
> + the pointer move. Re-uploads shrink from 50 GB to kilobytes.

### 1b. Interruption safety (the commit boundary)

The **pointer flip is the commit.** If a push dies after some chunks/the catalog but before the
pointer, the pointer still references the previous catalog → a concurrent restore reads the old,
fully-consistent version. A restore never sees a catalog whose chunks are missing. This is the
same "data first, index last" discipline we already applied to chunk-vs-catalog ordering.

### 1c. Live mirror (push on change, not just on connect)

Subscribe the holder push to `Coordinator.OnAnnounce` (the existing hook that fires on every
committed local model change). On connect, do the full reconcile; thereafter, each change
triggers an incremental reconcile to every connected holder peer.

**Coalescing (single-drain).** Per `(holder, folder)`: at most one push in flight and at most one
queued. A change during a push sets a `dirty` flag; the running push, on finishing, re-runs if
dirty. This prevents a burst of edits from stacking N pushes (the M5 notes warn a naive timer
debounce broke convergence — this is not a timer; it is a single-flight with a re-run flag, and
holder push is idempotent and last-writer-wins on the pointer, so it cannot diverge).

### 1d. Garbage collection of superseded blobs

Content-addressing means a changed catalog leaves the old catalog blob behind, and a deleted
file leaves unreferenced chunk blobs behind. The holder **cannot** GC itself (it can't read the
catalog to know what's referenced), so GC is **writer-driven**:

- Periodically (e.g. after the live mirror settles, or hourly, or on demand), the writer asks the
  holder to **list** its blinded ids (`opList`, paginated), diffs against the current needed set,
  and **deletes** the extras (`opDelete`, writers only).
- **Race guard:** only delete blobs the holder reports as older than a grace window (the holder's
  blob file mtime), so a chunk another writer just pushed for an as-yet-uncommitted catalog is
  never reaped. Mirrors the tombstone-reaping grace already in the sync engine. A reaped-too-eager
  chunk is self-healing anyway: the next push re-uploads it.

GC is correctness-optional (stale blobs only waste space); it ships but runs infrequently.

---

## 2. Parallel push (use QUIC's independent streams)

QUIC multiplexes many independent streams over one connection; we currently use one at a time.

- After §1's batch-HAS yields the absent chunk set, push those chunks across a bounded pool of
  **K concurrent streams** (K = 16, configurable), one chunk per stream, deduped by the needed
  set so workers never collide on an id.
- **Barrier, then catalog, then pointer.** Wait for all chunk pushes to succeed, *then* push the
  catalog blob, *then* the pointer. The barrier guarantees every chunk the new catalog references
  is durable before the catalog, and the pointer commits last.
- **Pipelining (refinement):** overlap discovery and transfer — start pushing the absent ids from
  HAS-batch *i* while issuing HAS-batch *i+1*. Ship the simple "HAS-all then push-parallel"
  first; add pipelining if measurement shows the HAS phase dominates.
- The holder server already caps concurrent handlers (semaphore = 64) and sheds beyond it, so the
  writer's K is bounded well under that; no new holder-side risk.

> **Example.** First backup of 4,000 files over a 100 ms link: serial wastes ~7 min purely
> waiting on round-trips; 16-way parallel cuts the round-trip stalls ~16×, leaving bandwidth as
> the limit.

---

## 3. Explicit restore command (`trove-peer restore`)

Decided UX: **explicit command** (not auto-on-connect).

```
trove-peer restore -dir <state> -group <groupID> -root <dest> \
    [-code <recovery-code>] -trove <connect-string> [-from <holder-node-id>]
```

Flow: open identity/config/membership → if `-code` is given, decode it to the master key and
store it (`DeliverFolderKey`, if-absent) and register the folder (`Holder:false`, `Encrypted:true`)
→ connect to a holder for the group → fetch+open the pointer → catalog → chunks (parallel,
verifying AEAD + BLAKE3 on each) → materialize bit-exact (the already-tested `holder.Restore`,
which validates the whole catalog before touching disk). Idempotent and resumable: re-running
restore re-fetches only what's missing locally.

### The open decision — how does the recovering node *authenticate to the holder*?

A holder only opens a session with a **roster member** (`Service.authorize` is roster-based), and
GET is served to any session-authorized peer. So a recovering node must be authorized. Two models:

- **Model A — roster membership.** The recovering node must be (re-)admitted to the group by a
  writer before it can pull from the holder. Simple, no new trust surface. **But** the whole point
  of a holder is recovery when *no trusted peer is online* — and then there is no writer to
  re-admit you, so Model A cannot recover from "only the holder survived." It only helps when a
  co-member is around — in which case normal sync from that co-member already restores you and the
  holder is unnecessary.

- **Model B — verifier-proven restore (recommended).** At holder setup the writer gives the holder
  the folder's **verifier** (`FolderVerifier(key, folderID)`) — a non-secret token from which the
  key cannot be derived. A node presenting the matching verifier is authorized **read-only** (GET /
  HAS / LIST, never PUT / DELETE) for that folder. This lets someone with only the recovery code
  recover when every trusted peer is gone — the actual disaster case. **Cost:** anyone who learns
  the verifier (it is broadcast to peers in `NetworkConfig`) can pull *ciphertext + blob sizes*
  from the holder; they still cannot decrypt without the key. So Model B trades "holder serves only
  roster members" for "holder serves anyone who can prove key-knowledge," narrowing confidentiality
  from content to *sizes-only-to-a-verifier-knower*. (A challenge-response that proves live key
  possession is impossible here: the holder has no key and so cannot check any answer; a stored
  static verifier is the only thing a keyless holder can check.)

**Decided: Model B.** The holder stores the verifier per folder (one non-secret value), exposes a
read-only session-auth path keyed by the verifier (GET/HAS/LIST only — never PUT/DELETE), and the
restore command presents the verifier derived from the recovery code. This is the only model that
recovers when no trusted peer is online — the holder's reason to exist.

---

## 4. Real-network holder test (the `make nat-matrix` holder cell)

Add one scenario to the existing real-network harness (containers behind simulated routers, real
hole-punching): a dedicated holder behind one NAT accumulates a writer's blinded ciphertext over a
real connection; the writer edits and the holder mirrors live; a recovering node behind another NAT
runs `trove-peer restore` and rebuilds the folder **bit-exact**; assertions confirm the holder dir
contains only ciphertext + blinded ids (no plaintext name/path/content) and that the holder never
received the key. This is the on-the-real-network confirmation of everything above.

---

## Wire protocol additions (golden-pinned; pre-release, no back-compat)

All on the existing `THLD` data-stream framing. Reads bound every length before allocating.

- `opHasBatch` (0x04): req `… ‖ uint16 count ‖ [32B blinded]*count` (count ≤ 4096); resp
  `status ‖ uint32 bitmaplen ‖ bitmap` (1 bit/id, present=1).
- `opList` (0x05): req `… ‖ 32B cursor` (zero = start); resp `status ‖ uint32 count ‖
  [32B blinded]*count ‖ 1B more`. Paginated for large folders. Read-only (member/verifier auth).
- `opDelete` (0x06): req `… ‖ uint16 count ‖ [32B blinded]*count`; resp `status`. Writers only.

Authorization: GET/HAS/LIST are read-only (members, or verifier under Model B); PUT/DELETE are
writers only. Each new op gets a golden layout test. `HolderVersion` stays 1 (additive, pre-release).

---

## Data-model & code touch-points

- `crypto`: catalog now content-addressed (reuse `Seal`); pointer uses `SealMutable`. No new
  primitives.
- `holder`: `wire.go` (+3 ops, golden tests), `server.go` (HAS/LIST/DELETE handlers + per-op auth;
  Model B verifier-auth path), `export.go` → split into `reconcile` (build needed set, batch-HAS,
  parallel push, pointer-commit) + `gc` (list/diff/delete), `restore.go` (pointer→catalog hop),
  `store.go` (already has `Delete`; add `List` with a cursor + mtime for GC grace).
- `node/sync.go`: `pushToHolders` becomes reconcile-based + an `OnAnnounce` subscription with the
  single-drain coalescer; periodic GC tick.
- `cmd/trove-peer`: `restore` subcommand; `invite -holder` gives the holder the verifier (Model B).
- `config`/`membership`: holder stores the verifier per folder (Model B) — a non-secret column.
- Tests: golden ops; incremental-skip (second push is a near-no-op); live-mirror (edit propagates
  without reconnect); interruption (kill before pointer → restore uses prior version); parallel
  push correctness; GC reclaims old catalog + unreferenced chunks without reaping in-flight;
  restore-with-recovery-code end-to-end; the real-network cell.

## Build phases (each green + reviewable)

1. **Catalog/pointer + stateless incremental reconcile + batch-HAS** (the headline; folds in the
   existing serial push). Tests: skip-unchanged, interruption.
2. **Parallel push** over reconcile. Tests: correctness + a perf sanity check.
3. **Live mirror** (OnAnnounce + coalescer). Tests: edit-propagates-without-reconnect.
4. **GC** (List/Delete + grace). Tests: reclaim without reaping in-flight.
5. **Restore command** (Model A or B per the decision). Tests: recovery-code end-to-end.
6. **Real-network holder cell** in `make nat-matrix`.

Each phase: WIP commit + background review subagents, then proceed — same loop as the rest of M6.

## Out of scope (future)

Multi-part (chunked) catalog to lift the ~700K-live-manifest single-blob ceiling; per-writer
signing (Tier B); key rotation. All compose cleanly on top.
