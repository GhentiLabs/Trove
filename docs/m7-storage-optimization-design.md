# M7 — Storage optimization (1× disk): design

How a node gets from M2's physical-only ingest (~2× disk: the working file **and** its
chunk copies in packed blobs) to ~1× without losing snapshot history. Deferred to M7,
co-located with retention, because that is also when history preservation becomes
load-bearing. This doc records the decision so it is not re-derived later.

## The governing predicate: write-mediation

Which 1× technique is safe is decided by **who writes the file**, not by how many writers
a folder has (writer-count is a separate axis that drives conflict resolution, not
storage):

- **Daemon-owned files (replicas).** The daemon writes via temp + atomic rename, so it
  controls every overwrite and has an "about to overwrite" moment. Any technique is safe
  here: hardlink, reflink, or virtual pointers.
- **User-owned files (owners).** The human overwrites in place; a passive FS-watcher
  learns of the change only *after* the old bytes are gone. There is no daemon
  "about-to-overwrite" moment, so any scheme that must copy a superseded chunk to safe
  storage **before** the overwrite ("Promote-before-overwrite") is **impossible** on the
  owner. Only **reflink/CoW** (the OS does the copy-on-write) or a **physical copy** is
  safe for owner files.

This is why M2's physical-only ingest on the owner is correct, and why the blueprint's
original "owner virtual backing + Promote" does not hold for a passive scanner.

## Decision

**Reflink/CoW with physical-copy fallback for *current* data; keep *history* packed.**
Uniform across owner and replica. **Promote-*before*-overwrite is never needed** — the clone
preserves the old bytes, so the only Promote that runs is *after* the overwrite, at re-ingest
(see "Promote-on-supersede").

Virtual-pointers were rejected as the primary mechanism: their only advantage is ~0 extra
bytes and cross-file dedup of current data, but they are fragile (a current-chunk read
re-hashes the live file and races every user edit) and they require the impossible
Promote-before-overwrite on the owner.

| | Virtual pointers | Reflink/CoW (chosen) |
|---|---|---|
| Owner (external edits) | unsafe (needs impossible Promote) | safe (OS does CoW; clone is an independent inode) |
| Serve a current chunk | re-hash live file; races edits | read the clone; stable |
| Disk, current data | ~0 extra | ~1× (shares extents until divergence) |
| Current-data dedup | sub-file + cross-file | intra-file only |
| Promote-before-overwrite | yes (impossible on owner) | never (clone preserves old bytes) |
| FS requirement | none | CoW FS, else fall back to physical copy |

The cross-file-dedup loss for *current* data is the only real cost and is small: identical
distinct files are rare, **history stays packed (fully deduped)**, and **wire-transfer
dedup is by `chunk_id` regardless of local layout**, so a newcomer never re-pulls a chunk
it already holds.

## Granularity: chunking and storage are orthogonal

CDC still runs to produce `chunk_id`s for the manifest (identity) and the wire (dedup).
Reflink changes only where the *bytes* live:

- **Current data:** one whole-file **clone object** per file. The index maps each
  `chunk_id → (clone_object, offset, len)` using the CDC boundaries.
- **History** (chunks in a retained snapshot, no longer in any current file): at the moment a
  change supersedes a chunk a retained snapshot still pins, that chunk is **promoted** out of
  its old whole-file clone into a deduplicated pack blob (sealed when the folder is encrypted),
  and the emptied clone is reclaimed. So history is packed, deduped deltas — an old version
  costs only the chunks it does not share with the current file. See "Promote-on-supersede".

macOS `clonefile()` is whole-file (one syscall, ~0 bytes). Linux `FICLONE` is whole-file;
`FICLONE_RANGE` (extent-level, Linux-only) is a possible later refinement for per-chunk
sharing — not in v1. CoW filesystems: APFS, btrfs, XFS (reflink=1), ZFS 2.2+, OCFS2,
bcachefs. Non-CoW (notably ext4) falls back to a full physical copy (= today's 2×).

## Control flow

**Owner ingest (file F changed):**
```
chunk F → chunk_ids + manifest          # identity/wire, unchanged from M2
clonefile(F, store/objects/<obj>)       # whole-file CoW clone; one syscall
  └─ non-CoW FS → io.Copy fallback (physical copy = today)
for (chunk_id, off, len) in F:          # index into the clone; no per-chunk byte copy
    index.put(chunk_id, BackingClone, obj, off, len)
# the previous clone is untouched by the OS, so old snapshot chunks stay valid. No Promote.
```

**Replica materialize (daemon-mediated):**
```
crash-safe apply (M4 Phase A): stage from pulled chunks → fsync → atomic rename into place
clonefile(dest, store/objects/<obj>)    # clone the just-written file
index current chunk_ids → (obj, off, len); drop the transient physical pulled chunks
# result ≈ 1× (working file + block-sharing clone)
```

Reflink works on both sides, so one uniform mechanism — no virtual-on-replica +
reflink-on-owner split. The replica could use pure-virtual safely (daemon-mediated), but
uniformity is worth more than the marginal bytes.

## `model` / GC interaction

`model` stays the sole reachability authority (reachable `chunk_id`s = live manifests ∪
retained snapshots). A **clone object** is reclaimable exactly when none of its `chunk_id`s
are reachable and it is past the grace age — the same shape as today's empty-blob
reclamation. A superseded chunk becomes unreferenced when the owner edits F and no retained
snapshot pins the old version; its old clone is then GC'd.

**No risk of collecting in-use data via sharing:** a `clonefile` clone is a *separate
inode*; the filesystem refcounts the shared physical extents and frees them only when both
the working file and the clone release them. Deleting a store clone never touches the user's
working file. This is strictly safer than virtual backing, where GC and serve reach into the
live file.

## What changes when

- **M4 (now): nothing in the storage layout.** Do not introduce a second on-disk
  representation during the first cross-machine convergence milestone. M4 ships
  physical-uniform; its complexity budget goes to the sync engine. The chunks schema already
  carries a per-chunk `backing` field and a separate locations table, so the optionality for
  a future `BackingClone` is already present — no migration, no pre-work.
- **M7 (built):** whole-file clone storage for current data, the `clonefile`/`FICLONE` path +
  physical-copy fallback, the `BackingClone` location kind, and the clone-object reclaimer.
  Clones are plaintext at rest (they must share the plaintext working file's extents), so
  current data — and snapshot-retained superseded data — is plaintext at rest even for an
  encrypted folder; transit, holder blobs, and recovery stay sealed. Replicas clone after the
  crash-safe materialize and reclaim the pulled chunks. The virtual-pointer machinery
  (`PutVirtual`, `MirrorFile`, `BackingVirtual`, `chunk_locations`) was pruned, confirmed
  unused.
- **Promote-on-supersede (built, follow-up to the 1× landing):** when a change drops a chunk
  that a retained snapshot still pins, the chunk is moved out of its old clone into a
  deduplicated pack blob at the moment of change — first-party in the ingest/apply path, not a
  deferred compaction pass. The clone is what makes this possible: the once-impossible
  "Promote-before-overwrite" becomes a Promote-after-the-fact, because the old bytes survive
  the overwrite in the clone and can be read back and repacked at re-ingest. This delivers the
  packed, deduped history the Granularity section describes; current data stays a clone (~1×).
  See "Promote-on-supersede" and "History mode".

## Promote-on-supersede

The clone preserves a file's old bytes through an in-place overwrite, so at re-ingest the
superseded chunks are still readable and can be lifted into deduped, packed history. This runs
first-party at the moment of change, not as a deferred GC/compaction pass.

**Mechanism (owner ingest and replica materialize alike):**
```
read prior manifest chunk ids for the path        # before committing the new version
clone the new file; point its chunks at the clone # current data stays ~1× (M7)
superseded = prior chunk ids − new chunk ids       # what this change dropped
history    = superseded chunks a retained snapshot still pins   # model anti-join
for id in history:                                 # chunkstore.Promote
    read the clone chunk's plaintext via Get
    store it through the Put path (compress, seal if encrypted, append + fsync)
    re-point its row BackingClone → BackingPhysical
reclaim any clone left referenced by no chunk row
```

The `history` set is the anti-join's job: a chunk a snapshot keeps **and** some current file
still references is *not* promoted (promoting it would strand the live file). A chunk kept by
no snapshot is left to GC. A deletion drops *all* of the prior version's chunks (the tombstone
still carries them), so deletions promote too. The owner reaches this path from both the
event-driven ingest and the rescan's deletion detection; the replica reaches it from a
superseding pull. It is best-effort: a crash between the commit and the promote leaves the
chunks as clones until their snapshot is forgotten and GC reclaims them — correct, just not yet
compact. Encrypted folders gain sealed history at rest as a side effect: a retained old version
stops being a plaintext clone once promoted.

## History mode

The founder picks a folder's purpose at `found`: **backup** (keep version history, the
default) or **sync** (keep none, true 1×). It is the data owner's call — they choose how to
spend the space their friends host — not the hosting machines'. `KeepHistory` both gates
whether the scanner cuts snapshots and selects promote-on-supersede's keep-vs-drop:

- **Backup:** snapshots are cut; superseded chunks a snapshot pins are promoted into deduped
  history. An old version costs only its changed chunks.
- **Sync:** no snapshots are cut, so nothing pins old versions; superseded chunks are dropped
  and reclaimed, and the folder stays at ~1×. Convergence is unaffected — it uses
  `CurrentRoot` (live leaves), not cut snapshots.

`config.Folder.KeepHistory` persists the choice; `cmd/trove-peer` exposes `-sync` at `found`
and `join`. Auto-propagation of the folder's mode to members, and a per-node "keep less than
the folder mode" override, are later refinements — today each node is told its mode at join.
