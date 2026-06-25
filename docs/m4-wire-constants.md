# M4 frozen wire constants

The M4 sync additions to the M3 protocol. Frozen the moment M4 ships; a change needs
a `wire_format_version` bump. Golden-pinned in `client/internal/wire/framing_test.go`
(control messages) and `client/internal/syncengine/codec_test.go` (data-stream
headers). See also `docs/m3-wire-constants.md` and the codec rule in the
m3-wire-codec-contract memory.

## Control-stream messages (protobuf bodies under the M3 framing)

Message type tags, append-only after the M3 set (NetworkConfig=1, Ping=2, Close=3):

| Type | Value | Direction | Purpose |
|------|-------|-----------|---------|
| `FolderSummary`   | 4 | owner → replica | announce snapshot root + resync cursor |
| `ManifestRequest` | 5 | replica → owner | request manifest delta since a cursor |
| `ManifestDelta`   | 6 | owner → replica | the requested manifests + cursor (epoch, high-water) |

`Folder` (in `NetworkConfig`) claims the two reserved cursor tags: `index_epoch_id`
(tag 4, uint64) and `high_water_sequence` (tag 5, int64). Tag 6 stays reserved (M6).

Cursor: `index_epoch_id` is the owner's per-folder stable epoch (random, persisted in
the owner's `folder_epoch` row; regenerated only if the owner's sequence numbering is
rebuilt). `high_water_sequence` is the owner's max manifest seq. A replica persists the
consumed `(epoch, high_water)` per `(folder, owner)` in `replica_cursors` and requests
`since_sequence = high_water`; an epoch mismatch forces a full resync (`since = 0`).

`RemoteManifest` carries the owner's identity verbatim (`manifest_id`,
`version_vector` as canonical bytes, `owner_sequence`, `deleted`), re-verified against
its chunk refs on apply. Control frames obey `MaxControlMessageSize` (1 MiB); large
full-resync deltas await manifest-delta pagination (M4 Phase C).

## Data-stream chunk protocol (raw bytes, hand-rolled header)

One chunk per QUIC data stream; the replica opens the stream, the owner serves it,
stream close = complete. Headers are big-endian and golden-pinned. Constants:
`DataMagic = 0x54445254` ("TDRT"), `DataVersion = 1`, `MaxFolderIDLen = 512`,
`MaxChunkBytes = chunker.MaxSize` (4 MiB), `msgKind` chunk = `0x01`.

Request (replica → owner), 40 + L bytes:

```
0   u32  DataMagic
4   u8   DataVersion
5   u8   msgKind (0x01 = chunk)
6   u16  folder-id length L (≤ MaxFolderIDLen)
8   …L   folder id (UTF-8)
8+L 32   chunk id (raw BLAKE3-256)
```

Response (owner → replica), 12-byte header then `length` raw plaintext bytes:

```
0   u32  DataMagic
4   u8   DataVersion
5   u8   status (0 ok, 1 not-found, 2 error)
6   u8   reserved (0)
7   u8   reserved (0)
8   u32  payload length (≤ MaxChunkBytes; 0 unless status ok)
```

The replica re-hashes the payload to the requested chunk id before storing; a mismatch
is discarded and refetched. `msgKind` and the reserved response bytes leave room for
later data-stream kinds (multi-source, membership) without changing this layout.
