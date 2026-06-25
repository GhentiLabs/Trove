# M3 frozen wire constants

These are the cross-node protocol contract introduced in M3 (`client/internal/wire`). The moment two
nodes exchange these, the layout is frozen: changing any of it is a wire-format break that requires
bumping `WireFormatVersion`. They join the same freeze-and-golden-pin discipline as the M2
identity/snapshot constants (`docs/m2-frozen-constants.md`), and are pinned by `framing_test.go`.

## Framing (hand-rolled, byte-exact)

- **Magic** = `0x54524F56` (`"TROV"`, big-endian). Distinct from BEP's magic. Prefixes the Hello
  frame for instant protocol identification and fast-rejection of non-protocol peers.
- **WireFormatVersion** = `1`. Carried in `Hello.wire_format_version`; an incompatible peer is
  rejected at Hello, never silently misparsed.
- **Hello frame:** `Magic (uint32) ‖ size (uint16) ‖ Hello protobuf`.
- **Post-Hello frame:** `header_len (uint16) ‖ Header protobuf ‖ msg_len (uint32) ‖ body`. The
  `Header{type, compression}` lets a peer route by type and decompress without parsing the body.
- All integers are **big-endian**.
- **MaxMessageSize** = `64 MiB`. Bounds a single post-Hello body on the wire; an oversize frame closes
  the connection. Sized with M4's larger index/manifest messages in mind.

## ALPN token (TLS)

`tls.Config.NextProtos` on both the dial and accept paths carries the Trove ALPN token
**`"trove/1"`** (`client/internal/transport`, const `alpn`). quic-go requires a non-empty
NextProtos; a peer offering a different token fails the TLS handshake. Frozen contract.

## Message types (frozen values)

Post-Hello `Header.type`: `NetworkConfig = 1`, `Ping = 2`, `Close = 3`. Message bodies are protobuf
(`wire.proto` → `wirepb`); golden-tested by parse + round-trip + version-gating, **not** by exact
emitted bytes (protobuf marshaling is not guaranteed deterministic; parse-compat is the contract).

## The codec rule (prevents an M4 signing bug)

Four categories, each with a fixed rule:

1. **Framing/envelope** → hand-rolled, golden-pinned byte-exact (above).
2. **Every structured message** → protobuf on the control stream.
3. **Chunk payloads (M4)** → raw bytes on dedicated QUIC data streams, no protobuf wrapper (one chunk
   = one data stream, stream close = chunk complete). The tiny data-stream header (chunk_id, ± length)
   is *also* frozen contract when M4 ships — golden-pin it even though it is raw, not protobuf.
4. **Anything hashed or signed** → **canonical hand-rolled bytes, never the protobuf encoding**, even
   when it travels inside a protobuf message. protobuf marshaling is not deterministic across
   maps/unknown-fields/versions, so signing protobuf bytes would make M4's signed membership entries
   unverifiable across peers. The signed payload (e.g. `node_id, role, added_by, added_at`) must be
   canonical bytes carried *inside* the protobuf envelope.

The clean line: **hashed/signed → canonical hand-rolled; merely transmitted → protobuf.**

## Reserved fields (do not repurpose)

`wire.proto` reserves, for forward evolution: `Folder` tags 4–6 (M4 `index_epoch_id`,
`high_water_sequence`; M6 `encryption_password_token`), and `NetworkConfig` tag 3 (advertised
addresses). M3 populates only `folder_id`, the default `folder_type`, `encrypted`, and the
per-connection `compression` preference.

## Regenerating

`make proto` (requires `protoc`; installs the pinned `protoc-gen-go`). The generated
`wirepb/wire.pb.go` is committed; contributors need protoc only when editing `wire.proto`.
