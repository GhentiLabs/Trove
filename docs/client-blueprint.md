# Client Blueprint — Full Engineering Design

The complete build blueprint for the peer daemon: how the system is layered, what each layer does, how it works internally, why it's built that way, and how data flows across the whole stack. Trove (discovery/signaling) is an existing dependency. Go, macOS-first then Linux, QUIC, no relays, full replicas, trusted network.

This is a reference document, not a build order narrative — §13 gives the pausable/testable build sequence. The wire-format RFC and the first-build spec are separate follow-ups.

---

## 0. Design stance (the decisions everything else inherits)

These are settled and load-bearing. The rest of the document assumes them.

1. **Content identity = `BLAKE3-256(plaintext chunk)`.** A chunk's identity is *what it is*, never *how it's stored*. Compression codec, encryption key, and at-rest representation are all local storage concerns that do not affect identity. This is the single most important decision; §1.2 and §1.3 explain the consequences.
2. **Integrity and identity are two mechanisms.** The plaintext hash carries *identity*; AEAD (ChaCha20-Poly1305) auth tags carry *ciphertext integrity*. We never overload one hash to do both jobs.
3. **Fixed pipeline order: chunk → compress → encrypt.** Never reversed; reversing destroys dedup.
4. **Immutable content-addressed snapshot is the first-class unit.** "Sync" = converge to the latest snapshot; "history/backup" = retain snapshots. Same mechanism.
5. **The engine is N-aware and conflict-aware from day one; execution ships one-way-first.** Version vectors, multi-source transfer, and conflict resolution are designed and built into the core so nothing is retrofitted — but the first milestones run folders in **one-way mode** (single owner, pull-only replicas), which exercises the entire stack while the conflict path stays dormant by construction. Flipping a folder to bidirectional lights it up. This gives an early working artifact without debugging live conflict resolution on day one, and without a painful bolt-on later.
6. **Network is a first-class object**, peer-held and gossiped now; server roster is a defined later upgrade (§15).
7. **Measure before fixing.** Several parameters (chunk-size defaults, index engine choice, HLC vs wall clock) are deliberately left as benchmarked open questions (§14), not guessed.

---

## 1. Layered architecture

Each layer depends only on layers below it. Upper layers are replaceable without touching lower ones; lower layers never call up. The dependency rule is strict — it's what makes the system testable in isolation and buildable in pausable increments.

```
 L10  Surface          control API · web UI · daemon supervision
 L9   Policy           folder modes (sync/backup/history/one-way) · retention · quota enforcement
 L8   Sync engine      reconcile · want/have · transfer scheduler · anti-entropy · conflict · receipts
 L7   Protocol         framing · codec · message catalogue · connection & session state machines
 L6   Discovery        Trove client (announce/lookup/signal) · mDNS · holepunch · address cache
 L5   Transport        QUIC + mTLS(pinning) · stream multiplexing
 L4   Membership       network object · signed roster · membership gossip · trust model
 L3   Local state      index DBs (namespaced) · scanner(watch+rescan) · GC · quota accounting
 L2   Model            manifest · snapshot · version vector · tombstone · diff
 L1   Content          chunker(FastCDC) · hasher(BLAKE3) · crypto(AEAD) · compression · chunk store
 L0   Substrate        identity(Ed25519) · time(std + synctest) · storage(SQLite helper) · netio(interface) · config
```

Data flows *down* on write (file → chunks → store) and *up* on read (chunks → manifest → file). Control flows *up* (a received message drives the sync engine which calls model and store). The two never tangle because content (L1) is immutable and addressed by hash — every higher layer refers to data by identity, not by location.

---

## L0 — Substrate

**Responsibility:** primitives every layer uses; the seams that make the system testable.

- **identity** — one persistent Ed25519 keypair. `node_id` = base32 of `SHA-256(SPKI)` (52 chars). Self-signed cert wraps the same key for QUIC/mTLS. The keypair is the node's sole identity for transport auth, membership signing, and addressing.
- **clock** — no clock seam. Runtime code uses the standard `time` package directly; timing is made testable with `testing/synctest` (Go 1.25+), which fakes time for code using real `time`. A local `nower` interface is introduced only in the rare spot synctest can't cover. Version vectors—not clocks—decide causality; the clock only timestamps receipts and breaks conflict ties. (HLC remains an open question §14, addressed at L2 when timestamps gain meaning.)
- **storage** — a concrete SQLite helper (`Open` with WAL pragmas, `WithTx`, `Exec/Query`), not an engine interface. Higher layers never touch the driver; engine-swap freedom (open question §14) comes from *containment* — all SQL lives in this helper and the store packages — not from an abstraction layer.
- **netio** — packet/stream send-receive behind an interface (the one substrate seam), so the deterministic simulator (§13.4) can substitute a virtual network with drop/delay/partition injection. synctest cannot fake network I/O, so this is the seam that earns itself.
- **config** — typed, versioned, on-disk config: this node's identity ref, networks, folders, peers, modes, quotas, key references. Mutations are transactional.

**Why:** build a seam only where a test genuinely cannot control the thing otherwise. synctest covers time and a temp dir covers storage, so neither needs an interface; only the network does. This avoids over-abstracting the substrate while keeping it fully testable.

---

## L1 — Content layer (immutable data plane)

**Responsibility:** turn bytes into content-addressed chunks and back, and store them efficiently. This layer knows nothing about files, folders, peers, or networks — only chunks and their identities.

### 1.1 Chunker — FastCDC, normalized
- **Algorithm:** Gear rolling hash, `fp = (fp << 1) + GearTable[b]` — one shift + add per byte. Boundaries are content-defined, so an insert near the front re-chunks only locally.
- **Normalized chunking (NC=2):** two masks. Before the average size, a *stricter* mask `maskS` (more 1-bits → boundary harder to hit → suppresses tiny chunks); after the average, a *looser* mask `maskL` (fewer 1-bits → boundary easier → suppresses giant chunks). This concentrates chunk sizes near the target and tightens variance versus basic CDC.
- **Parameters (current; §14 to revisit with data):** min 256 KiB, avg/normal 1 MiB, max 4 MiB, 64-bit Gear. Below min, skip boundary checks; at max, force a cut.
- **Determinism:** Gear table generated from a fixed seed (SplitMix64). Identical across nodes and languages — already verified bit-for-bit in the Go/Rust benchmark.
- **Interface:** `Split(reader) -> iterator<(offset, len)>`. Streaming; never loads the whole file.

### 1.2 Hasher — BLAKE3-256 of plaintext
- `chunk_id = BLAKE3-256(plaintext bytes)`. This is the chunk's universal name.
- BLAKE3's internal tree also enables verified streaming if ever needed, but identity uses the flat 32-byte root.
- **Why plaintext:** two peers can negotiate over the *same* `chunk_id` even when one stores plaintext and another stores ciphertext under a different key; rekeying or changing compression codec never changes identity; dedup is independent of storage form. (Contrast: hashing stored bytes makes identity representation-dependent and forces convergent encryption to be effectively mandatory.)

### 1.3 Crypto & compression
- **Compression:** zstd per chunk, after chunking, before encryption. Skip for already-compressed content types (a per-type flag, the only type-specific behavior anywhere). Each chunk records its codec id so a reader knows how to decompress.
- **Encryption (at rest / for untrusted transfer):** ChaCha20-Poly1305 AEAD per chunk. Per-folder master key (Argon2 from a passphrase or generated) → per-chunk key/nonce via HKDF keyed on `chunk_id`. AEAD tag authenticates the ciphertext.
  - **Convergent mode** (trusted folders): key derivation is deterministic from content, so identical plaintext → identical ciphertext → cross-node dedup survives encryption.
  - **Random-key mode** (untrusted peers): per-folder random key, no cross-node dedup with that peer, full opacity. Mode follows the trust flag automatically.
- **Stored form** of a chunk = `encrypt(compress(plaintext))` (or `compress(plaintext)` if folder unencrypted, or raw if also incompressible). Stored form is *local*; identity is the plaintext hash.

### 1.4 Chunk store
- **Logical map:** `chunk_id -> stored bytes`. Writing an existing `chunk_id` is a no-op (dedup).
- **Packing:** small chunks are grouped into pack blobs (~tens of MB); the index records `chunk_id -> (blob_id, offset, len, codec, enc)`. Prevents millions of tiny files.
- **Two physical backings, chosen per chunk:**
  - **Virtual (trusted nodes, current data):** `chunk_id -> (file_path, offset, len)` into a real working file. Current-version chunks are *not* duplicated into pack blobs — they're served by reading the live file. This is what stops a trusted node from double-storing its data.
  - **Physical (history + untrusted nodes):** chunks live in pack blobs in the object store. On a trusted node this holds only *history* chunks — those referenced by a retained snapshot but no longer present in any current file (the deltas). On an untrusted node, *all* chunks are physical and encrypted.
- **Serve path:** `Get(chunk_id, for_peer)` → resolve location → read (file or blob) → produce the wire form for the recipient (§7.4): plaintext to a trusted peer, folder-key ciphertext to an untrusted peer. A virtual read re-verifies the bytes still hash to `chunk_id`; on mismatch (file changed under us), it errors and triggers a rescan rather than serving stale bytes.
- **Why this matters:** identity-by-plaintext + virtual index together mean a trusted node stores its data exactly once (as real files) yet can still participate in content-addressed dedup, history, and chunk-level transfer.

**Open questions (L1):** chunk-size defaults (§14.1); index engine under GC churn (§14.2).

---

## L2 — Model layer (mutable control plane over content)

**Responsibility:** name files and folder states in terms of chunk identities; track causality.

- **FileManifest** — `{ path, type(file|dir|symlink), mode, mtime, size, []ChunkRef, version_vector, deleted }`, where `ChunkRef = { chunk_id, offset, len }` (offset/len describe position *within the file*, for reassembly). A file is reconstructed by concatenating its chunks in order. A **rename** changes only `path`; the chunk list is identical → zero data transfer.
- **Snapshot** — `{ root_hash, parent_root, created_at, created_by, manifest_set }`. `root_hash` = a Merkle hash over the sorted manifest set, so it uniquely names the *entire folder state*. Immutable. Snapshots form a DAG via `parent_root`. Structural sharing (unchanged manifests/chunks are reused) makes retaining many snapshots cheap.
- **VersionVector** — `{ []{ node_id, counter } }`. Per file. The editing node bumps its own counter. Comparison yields *dominates* / *dominated* / *concurrent*. This is causality; clocks are not used for it.
- **Tombstone** — `{ path, version_vector, deleted_at, expires_at }`. A delete is a versioned event with a lifetime longer than the worst-case offline window, so an offline peer can't resurrect a deleted file.
- **Diff** — two operations:
  - *Snapshot diff:* compare two `root_hash`es; walk only subtrees whose hashes differ → the set of changed manifests. Bandwidth scales with the change, not the dataset.
  - *Manifest-diff-against-base:* when sending an updated file, send the new manifest as a delta against a base the peer already has, so only changed `ChunkRef`s travel.

**Why:** the model is small and mutable; the content is large and immutable. All the hard stateful reasoning (causality, conflicts, deletes) happens here over tiny structures, never over bulk bytes.

---

## L3 — Local state layer

**Responsibility:** persist the model and the chunk index, keep them in sync with the real filesystem, reclaim space.

### 3.1 Namespaced index DBs (separate to avoid write contention)
- **config-plane** — networks, folders, peers, members, key refs, modes, quotas. Low write rate.
- **chunk-index** — `chunks` (each carrying `last_seen_ms`, not a refcount — see §3.3), `blobs`, `chunk_locations`. High write + GC churn. Isolated so GC sweeps don't contend with sync metadata; can use a different engine if measurement favors it (§14.2).
- **sync-state** — `snapshots`, `snapshot_files`, `files`(manifests), `version_vectors`, `tombstones`, `sync_receipts`, `transfer_progress`. Medium churn.
- **Why three:** the chunk index churns hardest (every write + every GC), config barely changes, sync-state is in between. Separating them keeps locks from colliding and lets each be tuned/replaced independently.

### 3.2 Scanner (filesystem → model)
- **Watcher** (`Watcher` interface): FSEvents on macOS, inotify on Linux. Debounce ~10 s (deletes ~1 min) to coalesce bursts.
- **Periodic rescan** (randomized ~hourly) always on: catches missed events, permission/mtime-only changes, and changes during downtime. Watch for latency, scan for correctness.
- **On detected change:** compare mtime/size/mode against the manifest in `files`. If changed → stream through the chunker → hash each chunk → store new chunks → build a new `FileManifest` → bump this node's version-vector counter → assign a new `seq` → write to sync-state → enqueue an index update and (on quiesce) a new snapshot.
- **Pipeline as bounded stages:** `scan → hashQ → storeQ → indexQ → announceQ`, each a bounded channel + worker pool. Backpressure propagates upstream (scanner slows when store is behind). This caps RAM — the named #1 scaling failure of naive sync daemons.

### 3.3 Garbage collection
- **Reachability is the sole authority** (no refcount). Mark the reachable `chunk_id` set from all *retained* snapshot leaves + current live manifests; a chunk not in that set is collectable. Refcounts were dropped because the chunk-index and sync-state are separate DBs (§3.1) — a refcount can't be made atomic with the manifest/snapshot change that justifies it, so it would structurally drift.
- GC is a low-priority background mark-and-sweep: read the reachable set from sync-state, then sweep unreferenced chunks from the chunk-index. **Grace age:** every `Put` (including a dedup hit) stamps the chunk's `last_seen_ms`; a sweep only deletes chunks last seen before `mark_start − grace`, and the delete re-checks `last_seen_ms < cutoff` atomically. Since a chunk is always `Put` (stamped) before any manifest references it, this protects in-flight/just-referenced chunks without a cross-DB lock — never sweep a chunk being written or referenced by an in-progress transfer.
- **Forget vs sweep:** forgetting a snapshot (dropping its rows) is cheap and frequent; sweeping (reclaiming chunks, deleting wholly-dead blob files) is expensive and infrequent. An interrupted sweep is safe — it only ever deletes provably-unreachable, past-grace chunks in independent transactions.

### 3.4 Quota accounting
- Per folder, account *logical* bytes (sum of unique chunk sizes referenced), accepting physical usage is lower due to dedup. Track pledged capacity and current usage; expose to policy (L9) for enforcement and pruning.

---

## L4 — Membership layer

**Responsibility:** define "a network" and who belongs, with trust.

- **Network** — first-class object: `network_id` (stable, derived from a network key/tag) + the signed member set. Held in the config-plane DB.
- **MembershipEntry** — `{ node_id, role(member|admin), added_by, sig, added_at }`. The set is self-authenticating: each change is signed by an existing member or admin, so any node can verify the roster without a central authority.
- **Roster gossip** — members exchange the entry set + per-node `last_seen` (the PEX/gossip pattern applied to membership). The roster converges across the mesh; each node caches it locally so the network keeps functioning with Trove down.
- **Trust model** — per peer, per folder: `trusted` (receives folder key, stores plaintext working files) | `untrusted` (receives only folder-key ciphertext + encrypted manifest, contributes storage, never reads). The trust flag drives the encryption mode (L1) and the wire form of chunks (L7.4).

**Why first-class now:** N-peer sync needs a membership concept regardless, and naming the network explicitly today makes the future server-roster upgrade a localized change (§15) instead of a retrofit.

---

## L5 — Transport layer

**Responsibility:** a secure, multiplexed, authenticated byte pipe between two `node_id`s.

- **QUIC** (quic-go): TLS 1.3 built in, multiplexed independent streams (no cross-stream head-of-line blocking — essential for parallel chunk transfer), connection migration when a peer changes networks (Wi-Fi↔cellular, roaming).
- **mTLS with fingerprint pinning:** both sides present their self-signed cert; each verifies the peer's cert fingerprint equals the expected `node_id`. No CA. A handshake whose fingerprint isn't a known/authorized member is dropped.
- **Stream model:** one long-lived **control stream** per connection (handshake, cluster config, index/have/want, gossip, ping) + many short-lived **data streams** (one per in-flight chunk or batch), so bulk transfer never blocks control traffic.

**Why QUIC, not TCP+TLS:** parallel multi-source chunk pull is the core workload, and QUIC's independent streams are exactly suited to it; the built-in TLS and migration are bonuses. (QUIC is *not* chosen for NAT traversal superiority — that's roughly equal to TCP; see §6.)

---

## L6 — Discovery & reachability layer

**Responsibility:** turn a `node_id` into a live QUIC connection, preferring the cheapest path.

- **Trove client** (existing server): `announce` (publish candidate addresses — LAN, UPnP/NAT-PMP-mapped, STUN-observed; learn the server-observed address), `lookup` (`node_id` → candidate addresses), `signal` (WebSocket broker: exchange candidates with a peer + a synchronized `punch_at_millis`).
- **Reachability ladder** (per peer, in order): (1) **mDNS** LAN — direct, zero infra; (2) **direct dial** to a cached or announced address; (3) **holepunch** via Trove `signal` — both sides fire simultaneous QUIC opens at `punch_at_millis`. **No relay** → symmetric/CGNAT-only pairs may fail to connect; accepted.
- **Address cache:** remember each peer's last working address and try it first; most reconnects skip discovery entirely.
- **Telemetry (opt-in):** post anonymized metrics to Trove `analytics` keyed by a random `install_id` (never `node_id`), bucketed — direct-vs-failed connection rate, NAT type, dedup ratio, transport, error counts.

**Why a ladder:** LAN and known-address paths are free and instant; holepunch is the fallback; the cache means discovery is mostly a first-contact cost.

---

## L7 — Protocol layer

**Responsibility:** the message contract and the state machines that run a connection and a sync session.

### 7.1 Framing & codec
- Post-mTLS, on the control stream: `hdr_len(u16) | header{ type, compression } | msg_len(u32) | protobuf(body)`. Messages are async, unordered, pipelined.
- Chunk payloads travel on data streams as length-prefixed opaque frames (already compressed+encrypted or plaintext per recipient) — *not* wrapped in protobuf, to avoid double-encoding bulk bytes.

### 7.2 Message catalogue (with directions; one-way subset marked ◑)
- `Hello { node_id, proto_version, name }` ↔ — first message each way.
- `NetworkConfig { []FolderOffer{ folder_id, mode, encrypted, trust, direction }, roster_version, []MembershipEntry }` ↔ ◑ — first post-auth message; declares folders, trust, and direction.
- `IndexRoot { folder_id, snapshot_root, []{ path, version_vector, seq } }` ↔ ◑ — announces current state.
- `ManifestReq { folder_id, []path, base_root? }` → / `ManifestResp { []FileManifest | manifest_delta }` ← ◑ — fetch manifests, optionally as a delta against a base the requester holds.
- `Have { folder_id, bloom | []chunk_id }` ↔ — set summary (bloom filter for large sets) of chunks held.
- `Want { folder_id, []chunk_id }` → ◑ — request specific chunks.
- `Chunk { chunk_id, wire_form, bytes }` ← ◑ — a chunk on a data stream; `wire_form ∈ {plaintext, folder_ciphertext}`.
- `MembershipGossip { network_id, roster_version, []MembershipEntry, []{ node_id, last_seen } }` ↔ — roster convergence.
- `SyncReceipt { folder_id, snapshot_root, direction, ts, bytes, chunks }` ↔ — exchanged on completing a folder sync; persisted both sides.
- `Ping {}` ↔ / `Close { reason }` ↔.
- **One-way folders** use only the ◑ subset in one direction (owner → replica): replicas send `Want`, receive `Chunk`/`ManifestResp`/`IndexRoot`; they never originate `IndexRoot` edits. This is the dormant-conflict execution mode.

### 7.3 Connection state machine
`Disconnected → Discovering → (Dialing | Punching) → TLS-Handshake → Authenticating(fingerprint) → ConfigExchange(Hello, NetworkConfig) → Active`. `Active` carries per-folder sync sessions. Terminal: `Closing → Disconnected`. Failures at any step return to `Disconnected` with backoff; the address cache and ladder drive the next attempt.

### 7.4 Wire form selection (the plaintext-identity consequence)
Because identity is the plaintext hash, the *bytes on the wire* depend on the recipient, not the sender's storage:
- **To a trusted peer:** send **plaintext** chunk bytes (safe — QUIC/TLS encrypts the link). The recipient applies its own at-rest pipeline (its codec, its key) and indexes by the same `chunk_id`.
- **To an untrusted peer:** the sender (who holds the key) sends **folder-key ciphertext**; the untrusted peer stores it opaquely under the given `chunk_id`, verifying the AEAD tag for integrity (it can't verify the plaintext hash without the key — acceptable in the trusted-network model; the receiving trusted nodes re-verify on read).
This is why hashing plaintext is the right call: one identity, many storage/wire forms, negotiated cleanly.

---

## L8 — Sync engine layer

**Responsibility:** drive folders to convergence across N peers.

### 8.1 Per-folder sync session state machine (per peer)
`InSync → Diffing → Pulling → Verifying → Applying → InSync`, with a `Conflict` branch off `Diffing`.
- **Diffing:** compare local vs peer `IndexRoot` (snapshot-root diff + per-file version-vector compare). Produce: files to fast-forward (peer dominates), files we're ahead on (we offer), and concurrent files (→ `Conflict`).
- **Pulling:** for needed files, resolve their manifests (request deltas against a base), compute the missing `chunk_id` set (those not already in our chunk store — local dedup first), and hand them to the scheduler.
- **Verifying:** every received chunk is hashed and checked against its requested `chunk_id` before use. Mismatch → discard + re-request from another source.
- **Applying:** see file-apply machine (§8.3).

### 8.2 Transfer scheduler (multi-source, N-aware)
- Maintains a global set of wanted `chunk_id`s and, per connected peer, which chunks that peer `Have`s (from bloom/list).
- Issues `Want`s across multiple peers in parallel, one data stream per chunk/batch, with a bounded in-flight window globally and per peer (backpressure).
- **Dedup in-flight:** don't request the same chunk from two peers unless one stalls (then re-issue to another — fastest-source-wins).
- **Rarest-first** ordering optional, so rare chunks propagate and the swarm stays healthy as peers come and go.
- Verify-by-hash on every receipt; a bad/byzantine source is caught automatically.

### 8.3 File-apply state machine (crash-safe)
`Needed → CopyLocal → FetchRemote → Verify → Finish`.
- **CopyLocal:** open a temp file; fill every chunk already available locally (old version of this file, or any other file/blob with the same `chunk_id`).
- **FetchRemote:** scheduler delivers missing chunks → written at their offsets in the temp file.
- **Verify:** confirm the assembled file's chunks match the manifest.
- **Finish:** fsync → set mode/mtime → **atomic rename** into place → update `files`/`chunk_locations` (current chunks become virtual locations into the new file) → adjust refcounts. Never index partial data.

### 8.4 Reconciliation timing — push + anti-entropy
- **Push:** on a local change, after re-chunk + new snapshot, send `IndexRoot` (and offer manifests) to all connected peers immediately — low latency.
- **Anti-entropy:** on (re)connect, exchange current `IndexRoot`s and reconcile from *current state* (roots + version vectors). No per-peer "changes since you left" log is needed; comparing current state is self-correcting regardless of downtime length. (A change journal is a possible later optimization, not a correctness requirement.)

### 8.5 Conflict handling (dormant in one-way mode)
- **Detect:** concurrent version vectors on the same path.
- **Resolve:** keep both — write `name.conflict-<ts>-<node_id>.ext` as a distinct file; pick a deterministic canonical winner (e.g., higher HLC/ts, tie-broken by `node_id`) so all nodes converge on which is primary. Never silently lose an edit. LWW available as an opt-in.
- **One-way folders never reach this branch**: a single owner originates edits; replicas only pull, so version vectors can't diverge. Enabling bidirectional mode activates the (already-built) path.

### 8.6 SyncReceipts (observability)
On completing a folder sync with a peer, both sides persist `SyncReceipt{ folder_id, peer_id, snapshot_root, direction, ts, bytes, chunks }`. This answers precisely "when did I last sync folder X to peer Y, and to what state" — drives the UI's last-synced display and the one-way-backup "safe to drop local copy" decision. Richer and more precise than `last_seen` gossip (which answers only "was this node alive recently").

---

## L9 — Policy layer

**Responsibility:** folder behavior as flags over the engine — no new mechanism.

- **Modes:** `sync` (keep latest, prune older), `history` (keep last N snapshots), `backup` (retain all; restore = materialize an old root), `one-way` (direction flag; replicas pull-only; may drop local chunks after a `SyncReceipt` confirms replication and re-pull on demand).
- **Retention/pruning:** on schedule or at quota, drop oldest snapshots (history/backup) which releases their now-unreferenced chunks to GC; or refuse new data (sync).
- **Quota enforcement:** reject inbound `Chunk` over the folder/node cap with a typed error; trigger pruning if policy allows.

---

## L10 — Surface layer

- **Local control API** (localhost): status, folders, peers, networks, snapshots, receipts, add/remove/configure. The daemon's command surface.
- **Web UI** (later, P2P-pure): served by each node from the control API; "remote view" = connect to *your own* node via Trove + holepunch; the server never sees file data or names.
- **Daemon supervision:** lifecycle, the bounded-queue/backpressure budget, graceful shutdown (drain in-flight transfers, flush index), crash recovery (replay from the last consistent snapshot + temp-file cleanup).

---

## 11. End-to-end data flows (traces across the stack)

**F1 — Add a folder (initial scan).** L10 add-folder → L3 scanner walks the tree → per file: L1 chunk+hash+store (current chunks become virtual locations into the working files) → L2 build manifests → L2 assemble the first snapshot (root over manifests) → L3 persist snapshot + manifests + version vectors. Result: a content-addressed snapshot of existing files with no data duplicated.

**F2 — Local edit replicates (the marquee flow).** L0 watcher fires → L3 debounce → scanner re-chunks the changed file (L1; only changed chunks are new) → L2 new manifest + bumped version vector → L2 new snapshot (shares unchanged chunks) → L8 push `IndexRoot`/manifest offer to connected peers (L7→L5) → peer L8 diffs, finds the changed manifest, computes missing `chunk_id`s (after local dedup) → peer L8 scheduler `Want`s them across sources → L1 serve path on the owner reads chunks (virtual → from the working file), emits plaintext (trusted) or ciphertext (untrusted) per L7.4 → peer verifies by hash, applies via the crash-safe file machine (L8.3) → both persist a `SyncReceipt`.

**F3 — Cold start.** L6 discover members (cached roster + Trove `lookup`) → L6 ladder connects to a live peer (mDNS/direct/punch) → L7 handshake + `NetworkConfig` → L8 anti-entropy: exchange `IndexRoot`s → diff → pull all missing chunks (multi-source if several peers up) → materialize files. A fresh node converges from whoever's online.

**F4 — Reconnect after offline.** Same as F3 but the diff is small: current-state comparison surfaces exactly what changed during the gap; no change-log needed.

**F5 — Concurrent edit (bidirectional folders).** Two nodes edit the same path offline → on reconnect L8 diff finds concurrent version vectors → `Conflict` branch → keep-both conflict file with a deterministic canonical winner → both converge on the same two files. (One-way folders never hit this.)

**F6 — Serve a chunk, trusted vs untrusted.** `Want(chunk_id)` → L1 resolve location → trusted recipient: read (virtual/blob), decrypt/decompress to plaintext, send plaintext; untrusted recipient: produce folder-key ciphertext, send opaque. Recipient indexes both under the same `chunk_id`.

**F7 — Multi-source pull.** Several peers `Have` the snapshot → L8 scheduler spreads `Want`s across them with a bounded in-flight window, rarest-first, fastest-source-wins on stalls; every chunk verified by hash. Throughput aggregates across peers; a bad source is caught and bypassed.

**F8 — Delete propagation.** File removed → L2 tombstone (versioned) → push → peers apply the deletion and keep the tombstone until `expires_at` → GC of the tombstone after all members converge. An offline peer reconnecting sees the tombstone and doesn't resurrect.

**F9 — Snapshot retention + GC.** Policy prunes the oldest snapshot → its manifests' chunk refcounts decrement (L3) → chunks now at zero refs across all retained snapshots + current files are swept by background GC → space reclaimed.

**F10 — Membership change.** Admin/member adds a `node_id` → signs a `MembershipEntry` → L4 gossip propagates it → all nodes verify the signature and update their cached roster → the new node, once it has the network key, is discoverable and authorized.

**F11 — Holepunch.** Both peers `announce` to Trove → A `signal`s for B → Trove hands each the other's candidates + `punch_at_millis` → both fire simultaneous QUIC opens → direct connection → L7 handshake. Trove never sees data.

---

## 12. Concurrency & backpressure model

- **Every pipeline stage is a bounded channel + worker pool.** Scan→hash→store→index→announce (write side) and want→fetch→verify→apply (read side). Bounded everywhere; backpressure propagates so RAM is capped regardless of folder size or peer count.
- **Transfer concurrency** is windowed globally and per peer; the scheduler never lets in-flight requests grow unbounded.
- **GC and scanning are low-priority** background work that yields to active sync and respects transaction boundaries.
- **One writer per index namespace** (or the chosen engine's transactions) to avoid contention; the three-namespace split keeps the hot chunk index off the config/sync locks.

---

## 13. Build order (pausable, testable, production-grade)

The engine is designed N-aware and conflict-aware throughout, but **execution ships one-way-first** so each milestone is a working, testable artifact and live conflict resolution is the last thing turned on. Every milestone has a deliverable, an acceptance test, and explicit stubs.

**M0 — Substrate.** ✅ *Built.* L0 (identity reuse, plain `time`/synctest, concrete `storage` SQLite helper, `netio` interface, `config`). *Accept:* a node has a stable `node_id`; storage is contained behind one helper; netio is substitutable; config round-trips. *Stub:* netio defined, unused; no real transport yet.

**M1 — Content store.** ✅ *Built.* L1 fully: FastCDC (frozen Gear table + masks), BLAKE3-plaintext, zstd, ChaCha20-Poly1305 + HKDF (convergent), pack blobs, virtual index. *Accept (met):* dedup proven, restore bit-exact (physical + virtual, encrypted + plaintext), encrypted-folder round-trips, virtual reads re-verify, boundary determinism + buffer-size independence + shift-resistance, corruption/tamper detection. Durability is power-loss safe (`synchronous=FULL` + blob/dir fsync). *Stub:* single encryption mode (no trust-driven mode yet); no virtual→physical promotion (M2). See `Docs/m1-content-store.md`.

**M2 — Model + local state.** ✅ *Built.* L2 + L3: manifests, snapshots, version vectors, snapshot diff, tombstones, scanner (fakeable watcher + fsnotify backend, debounce, snapshot-on-quiesce, periodic randomized rescan), namespaced DBs, GC, quota accounting. Single machine. *Accept (met):* edit a file → only that manifest+root change, every other manifest id byte-identical; snapshots share chunks (N small-delta snapshots ≪ N×); forget→sweep reclaims exactly the now-unreachable chunks; bounded pipeline holds RAM flat under a large synthetic tree; convergence + crash-recovery via rescan; history restores bit-exact. *Deviations from this blueprint (deliberate, reviewed):* (1) **GC dropped refcounts** for pure reachability mark-and-sweep guarded by a per-chunk `last_seen_ms` grace age — a refcount can't be kept consistent across the separate chunk-index and sync-state DBs without a cross-DB transaction, so it would structurally drift (see §3.1, §3.3 below). (2) **Deletion detection** streams live model paths and `lstat`s each (no in-RAM path set), rather than an event/refcount scheme. (3) **`ChunkRef` omits offset** (derivable from the running length sum). (4) **Ingest is physical-only**; virtual backing stays an unused chunkstore capability (history-safe), its no-history mode deferred to M3 — no M2 node needs it. (5) Watcher uses **fsnotify** now (kqueue on macOS); native FSEvents is the documented scaling upgrade. The orchestration/runtime that composes L2+L3 into a runnable daemon is **L10 surface**, not M2. Frozen wire/identity constants: see `Docs/m2-frozen-constants.md`.

**M3 — Transport + discovery + protocol handshake.** ✅ *Built (#4, #5).* L5+L6+L7 through `Active`: QUIC mTLS fingerprint pinning, Trove announce/lookup/signal, STUN reflexive discovery, holepunch, `Hello`/`NetworkConfig`. *Accept (met):* two real NATed machines establish a direct, pinned, authenticated session via Trove, exercised by the `e2e` CI (privileged netns/nftables cells running the real binaries) — punchable pairs connect, unpunchable pairs fail gracefully with no relay. Frozen constants: `docs/m3-wire-constants.md`.

**M4 — One-way sync (first shippable end-to-end).** ✅ *Built & merged (#6).* L8 in one-way mode + L4 membership (signed roster, gossip) + multi-source scheduler + crash-safe apply + SyncReceipts. Owner → N replicas, pull-only. **The first real, usable, testable system.** *Accept (met):* a multi-file folder converges across ≥3 peers including one offline for part of the run, through an edit, a delete, and a rename; multi-source pull with hash-verify; receipts recorded and "last synced" queryable. Proven in-process (MemNet+synctest and real-QUIC) and over **real holepunch** in the e2e matrix's 3-peer `offline-gate` cell (runs on every PR); the ≥3-machine run on real machines is the human sign-off (`docs/m4-live-runbook.md`). Shipped in phases (`docs/m4-roadmap.md`); golden constants in `docs/m4-wire-constants.md`.

*Deviations from this blueprint (deliberate, reviewed):*
1. **Membership is folder-as-group, not a separate network object.** A shared folder *is* a group; there is no per-peer guest list. `group_id = <founderNodeID>.<random-suffix>` commits to the founder, so any member derives and authorizes the founder from the id alone to bootstrap — replacing §4's `network_id` derived from a network key. Roles are Tahoe-style `reader`/`writer` (with `holder` reserved), not `member`/`admin`; the owner is the founding writer and root of trust. See `docs/m4-membership-design.md`. (The §15 server-roster upgrade and a dedicated per-folder network key stay the documented later evolution.)
2. **The conflict path is NOT pre-built.** Contrary to the "designed conflict-aware from day one; M5 just lights up the dormant path" stance (§0.5, §8.5), M4 ships pure one-way: replicas apply the owner's version vectors verbatim and never originate, so the conflict machinery (concurrent-VV detection, keep-both resolution) does not exist yet. The one-way *execution* makes that path unreachable-by-construction as intended — the deviation is that the code is absent, not flagged-off, so **M5 builds it rather than just enabling a flag.**
3. **Large single files paginate intra-manifest** (chunk-ref paging across delta frames), beyond Phase A's manifest-delta paging, so a file with more chunk refs than fit one control frame syncs.
4. **Deletion reaping is gated on receipts** (a tombstone is reaped only after every known replica's receipt has passed it; the retention window is the backstop), and replicas run a **startup repair** that re-materializes out-of-band-deleted files from local chunks.

> **Deferred: 1× storage optimization (was "virtual backing + `Promote`").** M4 keeps M2's physical-only ingest — owner and replica store current chunks in pack blobs as well as the working file (~2× disk), which makes "history preserved on edit" hold trivially. The §1.4 virtual-backing idea does **not** hold for a passive-scanner owner: the user overwrites a file before the daemon sees it, so `Promote`-before-overwrite is impossible. The 1× work is reframed around **reflink/CoW** (the OS does copy-on-write, so it is edit-safe on the owner with no `Promote`) and deferred to **M7** (retention), where history preservation becomes load-bearing. Physical storage is always correct, just larger. See `docs/m7-storage-optimization-design.md`.

**M5 — Bidirectional sync.** ✅ *Built (in-process; live NAT cell + human run pending).* The conflict machinery is new code, not a flag flip (M4 deviation 2): writers originate and bump their own version vectors, every node serves and pulls (gossip relay with per-peer cursors and local re-sequencing), concurrent vectors are detected and resolved keep-both, delete-vs-edit is unified, tombstones are two-way and reaping is membership-gated. Resolution is a pure function over agreed data (the two vectors, author node_ids, and an author edit-timestamp sidecar) with the unique node_id as the final tiebreak, so nodes converge with no coordination. *Accept (met in-process):* concurrent offline edits converge to byte-identical conflict copies on all nodes (order-independent + idempotent); delete-vs-edit keeps the edit; the one-way→bidirectional cutover preserves retained history bit-exact. Shipped in phases (`docs/m5-roadmap.md`); golden constants in `docs/m5-wire-constants.md`.

*Deviations from this blueprint (deliberate, reviewed):*
1. **Push is implemented now** (§8.4), not deferred: a model-change hook fans out through the per-folder coordinator so every session re-announces immediately, since two-way relay made the 5s-tick-only model visibly slow.
2. **Tombstone reaping is membership-aware** — gated on every roster member acking, not just peers seen so far — closing a mesh zombie-resurrection gap the seen-peers gate left open.
3. **Delete-vs-edit is edit-wins in place** (the edit stays at the real path, the delete is dropped) rather than resurrect-as-conflict-copy — the simpler deterministic rule that still never loses data.
4. **The conflict timestamp is wall-clock + node_id** (HLC deferred until measured skew shows wrong-winner picks); a `sent_ms` field on `FolderSummary` measures inter-node skew to drive that decision.

**M6 — Trust + encryption modes.** Trusted/untrusted per peer, convergent vs random keys, wire-form selection, key management + recovery key. *Accept:* an untrusted peer stores only ciphertext and can serve it back; a trusted peer reads plaintext; lost-key path documented.

**M7 — Policy & operations.** Modes (history/backup/one-way drop+re-pull), retention/pruning, quota enforcement, local control API. *Accept:* retention prunes + GC reclaims; quota rejects over-cap; restore from an old snapshot is bit-exact.

**M8 — Surface & hardening.** Web UI (P2P-pure), Linux support, parallel-transfer tuning, then the deterministic simulation harness (§13.4). I think i may also build out a rust UI for this. Lets talk and see where we are at here 

**13.4 — Testing as a first-class track.** Because time is fakeable with `testing/synctest` and the network sits behind the `netio` interface, build a deterministic simulator (synctest time, fault injection: drop/delay/partition/crash) and property tests over invariants: *total stored ≤ quota*; *no acknowledged data is unrecoverable while ≥K members are up*; *all nodes converge to the same snapshot set given no new writes*; *a chunk is never GC'd while referenced*. Run per-milestone from M2 onward.

---

## 14. Open questions (measure before fixing — do not guess these)

1. **Chunk-size defaults.** 256 KiB/1 MiB/4 MiB is a starting point. Measure dedup ratio vs metadata/index overhead vs transfer efficiency on real corpora before fixing. Smaller → better dedup, more index pressure; larger → less dedup, cheaper index.
2. **Chunk-index engine.** SQLite vs an embedded KV (bbolt/Badger) specifically under GC churn and high chunk counts. The three-namespace split lets this engine be chosen independently. Benchmark write amplification and GC sweep cost.
3. **Clock model.** Version vectors handle causality; but conflict tie-breaking and receipt/snapshot timestamps need a clock. There is no clock abstraction in the substrate (runtime uses `time` directly, tested via synctest); when L2 introduces meaningful timestamps, start wall-clock + `node_id` tiebreak and measure whether skew causes user-visible wrong-winner picks; adopt HLC if it does. Keep the timestamp field opaque-comparable so the swap is schema-free.
4. **Bloom vs explicit `Have`.** At what set size does the bloom summary beat sending explicit `chunk_id` lists? Measure false-positive re-request cost vs bandwidth saved.
5. **One-way → bidirectional cutover.** Confirm via simulation that enabling the dormant conflict path on a populated one-way folder behaves correctly before exposing it.

---

## 15. Future server-roster upgrade (out of scope to build now)

Add a durable roster to Trove: `network_id(blinded tag) → { member node_ids, last_seen }`, key-gated; join/list/leave endpoints; membership changes stored as signed assertions the server doesn't adjudicate (two policy questions: who may read, who may modify). **Client change is localized:** a "fetch roster from server" path feeding the same peer-side `{network → members}` map — identity, transport, sync, storage all unchanged. *Gains:* authoritative roster + clean cold-bootstrap (a fresh device with only the network key finds peers from nothing). *Cost:* server learns the grouping (mitigated by the blinded tag). *Trigger:* cold-bootstrap pain outweighing the metadata cost. Peers always keep a local roster cache, so a server outage never breaks an established network.
