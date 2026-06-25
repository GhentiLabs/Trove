# M4 Phase C — membership: signed roster + gossip (C1): design

Today a node authorizes peers from a hand-maintained `config.Peer` list. Membership
(L4) makes "the network" a first-class object: a **self-authenticating signed roster**
of members that **gossips**, so a node learns the full membership from any one peer
(introducer-style) and authorizes peers by roster, not by hand.

**C1 scope:** the signed roster, signature/identity verification, the trust chain, the
gossip exchange, and roster-driven authorization. **Deferred to C2:** `last_seen`
liveness gossip, auto-dialing roster members (discovery from the roster), and admin
revocation. **Out (M6):** untrusted peers, encryption, key-agreement. C1 is trusted-only.

## Trust model: self-rooted chain

A network is founded by an **admin** whose entry is self-signed. `network_id` is the
founder's `node_id` — the root of trust a joiner is given out-of-band (a short string).
Every other entry is signed by an existing admin, forming a delegation chain rooted at
the founder. (A dedicated network keypair, decoupling `network_id` from any node, is a
later hardening; for C1 the founder's node key is the root — founders are stable.)

The binding problem: a `node_id` is `base32(SHA-256(SPKI))` — a *hash*, so a verifier
needs the signer's **public key**, not just its id. Therefore each entry carries the
member's Ed25519 public key, and verification checks `FingerprintKey(publicKey) ==
node_id` (a new `identity.FingerprintKey`, matching `FingerprintCert`).

## MembershipEntry

```
MembershipEntry {
  network_id   string        // the founder node_id this entry belongs to
  node_id      string        // the member
  public_key   []byte        // the member's Ed25519 public key (32 bytes)
  role         Role          // member | admin
  added_by     string        // node_id of the signer (an admin); == node_id for genesis
  added_at_ms  int64
  sig          []byte        // Ed25519 over the canonical bytes below, by added_by
}
```

**Signature payload = canonical hand-rolled bytes, never protobuf** (the frozen codec
rule): a domain tag (`trove/membership/v1\x00`) then length-prefixed, fixed-order
`network_id ‖ node_id ‖ public_key ‖ role ‖ added_by ‖ added_at_ms`. Golden-pinned.
Protobuf is only the transport envelope, never the signed bytes.

**Verification of an entry E:**
1. `len(public_key)==32` and `FingerprintKey(public_key) == E.node_id`.
2. `network_id == this network`.
3. `ed25519.Verify(addedByPublicKey, canonical(E), E.sig)`, where `addedByPublicKey` is:
   - E's own `public_key` if `added_by == node_id` (genesis self-signature) **and**
     `node_id == network_id` and `role == admin` (only the founder may self-sign);
   - else the `public_key` of an already-verified **admin** entry whose `node_id ==
     added_by`.

So a member entry is accepted only if signed by a verified admin, transitively rooted
at the founder (`network_id`). An entry failing any check is **rejected and not stored
or propagated**.

## Roster store (config-plane)

A `membership` package over the config DB: a `members` table
(`network_id, node_id, public_key, role, added_by, added_at_ms, sig`, PK
`(network_id,node_id)`). Operations:
- `Found(network)` — create a network: generate the founder's self-signed admin entry
  (the local node is the founder) and store it; `network_id = local node_id`.
- `Join(network_id)` — record the network_id to anchor verification; roster fills via gossip.
- `Add(node_id, publicKey, role)` — an admin signs and stores a new entry (local key).
- `Verify(entry)` / `Merge(entries)` — verify against the stored roster + root, insert
  the valid, reject the rest; return which were newly added (to re-propagate).
- `Roster(network_id)` / `IsMember(network_id, node_id)`.

Verification needs the chain; `Merge` repeatedly admits entries whose `added_by` is
already a verified admin until no more can be admitted (handles out-of-order gossip).

## Wire + gossip

New control message `MembershipGossip { network_id, []MembershipEntry }` (protobuf
envelope; entries carry their canonical-signed `sig`). On an Active session, each side
sends its roster for shared networks on connect and on a periodic anti-entropy tick;
on receipt, `Merge` verifies + stores, and newly-admitted entries are re-gossiped to
other sessions (introducer propagation). Invalid entries are dropped silently (logged
at debug), never propagated. Bounded like the manifest delta (page if it ever exceeds
the control-frame cap — reuse the pagination pattern; rosters are small in C1).

## Integration: authorize by roster

`node.authorize(nodeID)` additionally returns authorized if the peer is a verified
member of a network whose folders this node shares. C1 keeps the existing `config.Peer`
path too (either grants); C2 can retire manual peers. Auto-dialing roster members
(feeding `peermgr.Peers` from the roster) is C2.

## Tests (deterministic, repo conventions)

- **Crypto/golden:** canonical bytes golden-pinned; `FingerprintKey(pub)` equals
  `FingerprintCert` for the same key; sign→verify round-trip; a tampered field or sig
  fails; a public key whose fingerprint ≠ node_id is rejected.
- **Chain:** founder self-signed accepted; admin-signed member accepted; member-signed
  entry (non-admin signer) rejected; an entry for the wrong network_id rejected; a
  forged entry (valid sig by a non-member key) rejected; out-of-order merge converges.
- **Gossip (3 nodes over MemNet):** founder adds A; B (joined with only `network_id`)
  learns the full roster from A alone (founder offline); an injected invalid-signature
  entry is rejected and not propagated.
- **Authorize:** a verified roster member is authorized; a non-member is not.

## Risks / edges

- Founder rekey changes `network_id` (accepted C1 limitation; dedicated network key is
  the fix). Revocation/removal is not in C1 (entries are add-only); an evicted node
  stays in the roster until C2 revocation — acceptable for trusted C1.
- Gossip must converge regardless of arrival order (the fixpoint `Merge`).
- A peer flooding bogus entries: each is signature-checked before storage, so the cost
  is CPU per entry; bound the gossip message size (control-frame cap) and entries per
  message.
