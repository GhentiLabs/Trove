# M2 — Frozen wire & identity constants

These values define cross-node identity and are exchanged (M3/M4) over the wire.
They are **frozen**: changing any of them changes computed ids/roots and breaks
dedup, structural sharing, and cross-node agreement. A change requires an explicit
format-version bump and a migration. Golden tests pin every value below.

## Manifest identity (`client/internal/manifest`)

`ManifestID = BLAKE3-256(identity bytes)`. The identity bytes are, in order:

```
"trove/manifest/v1\x00"            domain tag (frozen)
byte   formatVersion = 0x01
byte   kind                        0=regular 1=dir 2=symlink
uvarint len + path bytes           NFC-normalized, folder-relative, '/'-separated
byte   canonicalMode               regular: 0=0o644, 1=0o755 ; dir/symlink: 0
uvarint len + symlinkTarget bytes  NFC-normalized; empty unless kind=symlink
uvarint chunkCount
  per chunk, in file order: 32 raw ChunkID bytes + uvarint plaintextLength
```

**Hashed (identity):** kind, NFC path, canonical mode, NFC symlink target, ordered
chunk refs. **Never hashed:** mtime, size (derivable), inode, version vector,
sequence, deleted flag — they vary per machine/write and would break
"same tree → same root across machines".

- **Canonical mode:** git-style minimal set — only the executable bit of regular
  files (`mode & 0o111`). The full raw mode is stored for restore, never hashed.
- **Path normalization:** Unicode **NFC**, applied to path and symlink target
  before hashing/indexing. **Case-sensitive / case-preserving.**
- Integers little-endian; variable-length fields uvarint length-prefixed.

**Version vector** canonical bytes: `uvarint(n)` then, sorted ascending by node id,
`uvarint(len)+nodeID + uvarint(counter)`; zero-counter entries dropped. Node id is
the 52-char lowercase base32 fingerprint from `pkg/identity`. Stored only; never
part of the manifest identity.

## Snapshot root (`client/internal/snapshot`)

Binary Merkle root over path-sorted leaves, domain-separated:

```
leaf  = BLAKE3("trove/snapshot/leaf/v1\x00" || uvarint len + NFC path || manifestID(32) || deleted byte)
node  = BLAKE3("trove/snapshot/node/v1\x00" || left(32) || right(32))
empty = BLAKE3("trove/snapshot/empty/v1\x00")
```

Build: one leaf per path, sorted by NFC path, reduced as a balanced binary tree;
an odd node carries up unchanged (RFC-6962 — no Bitcoin-style duplication). The
root is **pure content state**: it excludes the version vector, wall clock, and
parent link (those are stored snapshot columns), so identical states share a root.
A leaf commits the path, manifest id, and tombstone flag.

## Chunk identity (`client/internal/hasher`, `client/internal/chunker`) — M1, unchanged

`ChunkID = BLAKE3-256(plaintext)`. Content-defined chunking is FastCDC with frozen
gear table and masks (`MinSize=256 KiB`, `AvgSize=1 MiB`, `MaxSize=4 MiB`). Per-chunk
convergent encryption derives key+nonce via HKDF-SHA256 keyed by the chunk id, so
identical plaintext yields identical ciphertext (dedup survives encryption).

## On-disk schema versions (local, not wire — migrated forward)

- `chunkstore` **v2**: chunks carry `last_seen_ms` (no refcount). Reclamation is
  reachability mark-and-sweep guarded by a grace age over `last_seen_ms`.
- `model` v1, `config` v1.
