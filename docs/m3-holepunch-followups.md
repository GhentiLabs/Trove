# Hole-punch follow-ups (deferred past M3)

M3 ships the hole-punch *mechanism* and meets its accept criterion — two NATed peers establish a
direct, pinned, authenticated QUIC session via Trove. This note records the connectivity work
**deliberately deferred**, so it is not lost and is tackled in priority order later. None of it is
built in M3; M3 targets the *punchable* population only.

## Where M3 stands vs the state of the art

Our punch mechanics match libp2p's DCUtR on the details that are easy to get wrong:

- One UDP socket shared by the NAT-opening probe and the real QUIC connection (the punched mapping is
  the mapping QUIC uses).
- Probe-and-accept role split: the deterministic non-dialer fires a probe to open its mapping while the
  other side dials (`peermgr/ladder.go`, `node.punchInbound`).
- Synchronized open via a server-brokered `punch_at_millis`.
- A UPnP/NAT-PMP mapping fast path, plus richer candidates (LAN + mapped + server-observed reflexive)
  than DCUtR's CONNECT exchange carries itself.

libp2p's large-scale field measurement (punchr, ~4.4M attempts across 85k networks, arXiv 2510.27500)
puts coordinated hole punching at **~70% success**, with TCP and QUIC statistically indistinguishable.
The remaining **~30% is physically unpunchable** — symmetric NAT on at least one side, dual-CGNAT, or
networks that block outbound UDP — and is reachable **only via a relay**.

## No relay, by design

Trove is **strictly peer-to-peer for data**: the discovery server only *coordinates* connection setup
(discovery, signaling, STUN reflexive). Peer file/data traffic **never** flows through Trove
infrastructure. We therefore **do not** and **will not** add a TURN/DERP-style data relay — that would
route peer data through the server, which is a non-goal. The consequence is permanent and accepted: the
physically-unpunchable ~30% (symmetric×symmetric, dual-CGNAT, UDP-blocked) **cannot connect**, and
that is the correct final behavior, not a gap to close. The deferred items below all improve
*direct-punch success* without ever relaying data.

## Deferred work, in priority order

1. **Dual-reflexive NAT classification.** One server-observed reflexive address cannot distinguish an
   endpoint-independent-mapping NAT (punchable) from a symmetric one (not). Observe from **two** server
   ip:ports (or two servers) and compare the mapped ports to classify the NAT (RFC 4787 mapping
   behavior). Lets us detect a symmetric peer up front and stop attempting a punch that cannot succeed,
   instead of retrying until timeout.

2. **Birthday-paradox port prediction.** For the endpoint-independent ↔ symmetric case, open ~256 local
   ports while the peer fires ~1024 probes (~98% collision when only one side is symmetric, per
   Tailscale). This is the *only* way to reach a one-sided-symmetric peer without a relay, so it is the
   highest-value direct-only improvement. Noisy and complex; measure before adopting.

3. **RTT-relative punch timing.** Replace the absolute `punch_at_millis` (which assumes both peers'
   wall clocks agree) with libp2p's approach: measure RTT over the coordination channel and dial after
   `rtt/2` so both sides' first packets cross mid-path. Clock-skew-immune. (M3 relies on reasonably
   synced clocks; NTP-grade is sufficient in practice.)

4. **Role-switch on the last retry.** libp2p flips the dial/accept role on its final attempt to interop
   with peers that picked the opposite role. Our role is a deterministic id comparison so both sides
   already agree, but a final role-flip is cheap insurance against edge cases. (The basic retry loop —
   several tightly-spaced coordinated rounds per attempt — is **already implemented** in
   `peermgr/ladder.go`; only the role-flip refinement is deferred.)

## Testing note

These are validated by the on-demand Docker netns e2e matrix (`make e2e`): the symmetric and
CGNAT cells must **fail gracefully** (surfaced, not hung, no relay) — and, because Trove never relays,
that is their permanent expected outcome. Real-world success rates are only knowable from a live
multi-network run (cf. libp2p's `punchr`).
