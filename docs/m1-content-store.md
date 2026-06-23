# M0 Substrate + M1 Content Store — Reference

What was built in the first milestone of the client (`client/`), the lowest two
layers of the peer daemon. Single machine, no networking. Use this as the
integration reference for M2 (model + local state).

Module: `github.com/GhentiLabs/Trove` · Go 1.26 · pure-Go deps only.

## Package map

```
client/
  internal/
    storage/       SQLite boundary: Open (WAL pragmas), WithTx, Exec/Query.
    netio/         Stream/Dialer interfaces only (transport seam; unused in M0/M1).
    config/        Versioned config DB: node identity ref + folder registry + folder keys.
    hasher/        BLAKE3-256 chunk identity (ChunkID).
    chunker/       Normalized FastCDC, streaming, buffer-size independent.
    compression/   Per-chunk zstd codec (pooled), with CodecNone fallback.
    crypto/        Convergent per-chunk AEAD (ChaCha20-Poly1305 + HKDF) + Argon2id.
    chunkstore/    Pack blobs + chunk index; Put/Get, physical + virtual, ingest/reassemble.
  cmd/
    trove-chunk/   Manual harness: print a file's chunk offsets/lengths/ids.
```

Decisions that differ from the original blueprint wording: **no clock seam**
(plain `time`, tested with `testing/synctest`); **netio is the only substrate
interface**; **storage/config are concrete** (engine-swap freedom via containment,
not abstraction); **flat package layout**.

## Public APIs

**storage** — `Open(Options{Path, MaxOpenConns}) (*DB, error)`; `(*DB).Exec/Query/QueryRow(ctx,…)`;
`(*DB).WithTx(ctx, func(*Tx) error) error` (serializes writers; commits on nil, rolls back on error/panic).

**config** — `Open(Options{DB, NodeID}) (*Store, error)`. `NodeID()`,
`AddFolder/GetFolder/ListFolders/RemoveFolder`, and keys:
`GenerateFolderKey`, `DeriveFolderKey(id, passphrase)`, `GetFolderKey`, `SetFolderKey`.
Errors: `ErrFolderNotFound`, `ErrFolderExists`, `ErrNoKey`, `ErrSchemaTooNew`, `ErrNodeMismatch`.
Node identity comes from `pkg/identity` (`LoadOrCreateKey` → `LoadOrCreateCert` → `FingerprintCert`).

**hasher** — `type ChunkID [32]byte`; `Sum([]byte) ChunkID`; `String()` (hex),
`Parse`, `FromBytes`, `Bytes`; streaming `Hasher` (`New/Write/Sum/Reset`).

**chunker** — `New(Options{Reader, BufSize}) *Chunker`; `Next() (Chunk, error)`,
`NextChunk() (Chunk, []byte, error)`, `Split([]byte) []Chunk`. `Chunk{Offset, Length}`.

**compression** — `type Codec uint8` (`CodecNone`, `CodecZstd`);
`Compress(src) (Codec, []byte)` (smaller of zstd/original); `Decompress(codec, data) ([]byte, error)`.

**crypto** — `Seal(master [32]byte, id ChunkID, data) ([]byte, error)`,
`Open(master, id, ciphertext) ([]byte, error)`, `DeriveMasterKey(passphrase, salt) [32]byte`.

**chunkstore** — `Open(Options{DB, BlobDir, Logger, BlobTargetSize}) (*Store, error)`.
- `Put(ctx, FolderContext, plaintext) (ChunkID, error)` — physical, deduped (refcount bump on repeat).
- `PutVirtual(ctx, id, filePath, offset, length, plaintextLen) error`.
- `Get(ctx, FolderContext, id) ([]byte, error)` — verified plaintext, either backing.
- `Has`, `Close`.
- `ImportStream/ImportFile(ctx, FolderContext, …) ([]ChunkID, error)` — physical ingest.
- `MirrorFile(ctx, path) ([]ChunkID, error)` — virtual ingest, no byte copy.
- `Reassemble(ctx, FolderContext, ids, w) error` — bit-exact restore.
- `FolderContext{Encrypted bool, MasterKey [32]byte}` carries the per-op encryption decision;
  the store never imports `config` — the caller resolves the key and passes it in.
- Errors: `ErrChunkNotFound`, `ErrHashMismatch`, `ErrFileChanged`, `ErrNoKey`.

## Storage pipeline

`chunk → compress → encrypt`, never reversed. Identity is `BLAKE3-256(plaintext)`;
integrity is the AEAD tag — separate mechanisms. Read path always reverses the
pipeline and re-hashes, returning an error rather than wrong bytes.

## Frozen protocol constants (never change — they define chunk identity)

- Gear seed `0x2545F4914F6CDD1D`; table built by SplitMix64. Golden: `gear[0]=0xC0E16B163A85A4DC`.
- `MaskS = 0x954AA552A9550000` (popcount 22), `MaskL = 0x924A494929250000` (popcount 18).
- Sizes: `MinSize=256 KiB`, `AvgSize=1 MiB`, `MaxSize=4 MiB`.
- crypto HKDF: `info = chunk_id || "trove/chunk/v1"`, fixed salt `"trove/chunk-keys"`.

Tunable (not frozen): `BlobTargetSize=64 MiB` (default; overridable per store).

## Invariants worth knowing for M2

- Convergent encryption: identical (master key, plaintext) → identical ciphertext, so
  dedup survives encryption. The per-chunk key is unique because it is keyed by the
  plaintext identity; do not move to a per-folder key or reuse keys.
  Tradeoff: a store reader can confirm a *known* plaintext is present.
- Durability: client DBs open with `synchronous=FULL`, blob bytes are fsynced before the
  index row commits, and a new blob's directory entry is fsynced before its `blobs` row is
  committed. This holds against both process crash and OS crash / power loss. A crash can
  still leave an orphan tail in the open blob, which `Open` truncates to `blobs.size`; a
  committed index row never points past durable bytes.
- `refcount` increments on duplicate Put; nothing decrements it yet — M2 GC owns that.
- Virtual chunks are always `codec=None, encrypted=0` (raw working-file bytes); a virtual
  read re-hashes the file and returns `ErrFileChanged` on mismatch (or `ErrChunkNotFound`
  if the backing file is gone).
- Secrets at rest: folder master keys are stored as plaintext BLOBs in the config DB, and
  plaintext-folder pack blobs hold user bytes in the clear. This is acceptable under the
  trusted single-machine model — the same trust level as the working files and the Ed25519
  identity key. The store keeps DB files, the `-wal`/`-shm` sidecars that exist at open, and
  blob files/dirs owner-only (0600/0700). The daemon should additionally place all client
  state under a 0700 directory so sidecars created later by checkpoints stay protected;
  per-column key encryption is deferred to the M6 key-management milestone.

## Database schemas

**config DB**: `meta(key,value)` (schema_version, node_id) and
`folders(id, root, encrypted, master_key, kdf_salt, kdf_time, kdf_mem_kib, kdf_threads, created_ms)`.

**chunkindex DB**: `meta(key,value)`;
`chunks(chunk_id BLOB PK, backing, blob_id, blob_offset, length, codec, encrypted, plaintext_length, refcount)`;
`blobs(blob_id PK, path, size)`;
`chunk_locations(chunk_id, file_path, file_offset, length)`.
Chunk ids are stored as raw 32-byte BLOBs.

## Status

All packages build, `go vet` clean, and pass `go test -race ./client/...`. Coverage
includes the M1 acceptance criteria: boundary determinism + buffer-size independence,
shift-resistance, dedup on small edits, bit-exact restore (physical + virtual, encrypted
+ plaintext), and corruption/tamper/file-change detection.

## Where M2 plugs in

- Manifests/snapshots reference chunks by `ChunkID`; build them from the ordered id lists
  returned by `ImportFile`/`MirrorFile`.
- The scanner ingests via the same store; new working files use `MirrorFile` (virtual),
  imported/history bytes use `ImportFile`/`Put` (physical).
- GC will decrement `refcount` and sweep `blobs`; the index is the source of truth.
- Sync-state belongs in a third DB file opened via `storage.Open` (same pattern as config
  and chunkindex), keeping the hot index isolated.

Known gaps to close as M2 builds on this (surfaced by review, deferred by scope):
- **Promote(virtual→physical):** the edit/history path needs to turn a current (virtual)
  chunk into a physical pack-blob chunk when its working file changes. The schema supports
  it; the store needs the method (and a backing-mismatch guard so a chunk can't be both).
- **Atomic refcount vs GC:** `Put`'s dedup does `Has`+`bumpRefcount` separately. Safe today
  (single writer, no GC), but GC must make refcount changes atomic with the sweep (single
  `INSERT … ON CONFLICT DO UPDATE`, as `PutVirtual` already does) to avoid losing a chunk.
- **Blob fd caching:** `Get` opens the blob file per call; multi-source reassemble in later
  milestones will want a small descriptor cache.
