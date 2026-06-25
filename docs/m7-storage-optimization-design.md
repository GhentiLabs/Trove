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
Uniform across owner and replica. **`Promote` is never needed.**

Virtual-pointers were rejected as the primary mechanism: their only advantage is ~0 extra
bytes and cross-file dedup of current data, but they are fragile (a current-chunk read
re-hashes the live file and races every user edit) and they require the impossible
Promote on the owner.

| | Virtual pointers | Reflink/CoW (chosen) |
|---|---|---|
| Owner (external edits) | unsafe (needs impossible Promote) | safe (OS does CoW; clone is an independent inode) |
| Serve a current chunk | re-hash live file; races edits | read the clone; stable |
| Disk, current data | ~0 extra | ~1× (shares extents until divergence) |
| Current-data dedup | sub-file + cross-file | intra-file only |
| Promote needed | yes (impossible on owner) | never |
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
- **History** (chunks in a retained snapshot, no longer in any current file): packed blobs,
  as today.

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
- **M7:** whole-file/large-object storage for current data, the `clonefile`/`FICLONE` path +
  physical fallback, the `BackingClone` location kind, and the clone-object reclaimer.
- **Open decision at M7:** the existing virtual-pointer machinery (`PutVirtual`,
  `MirrorFile`, `BackingVirtual`, `chunk_locations`) is currently unused. If reflink is
  confirmed, prune the virtual path then rather than carry it as latent bloat. Left in place
  until the M7 mechanism is final (it is tested and harmless).
