# M4 Phase A — live convergence runbook (owner → replica)

The deterministic in-process tests are the gate CI runs. This runbook is the live,
cross-machine check (run by a human, like the M3 holepunch gate): an owner and a
replica on two real machines converge a folder over Trove + holepunch, peer-to-peer.

Prerequisites: `trove-peer` built on both machines (`make build`), a reachable Trove
discovery server URL (`trove://host:port?id=<fingerprint>`).

## 1. Learn each node id

On each machine, in its state dir, print the node id (no `-trove` ⇒ prints and exits):

```
trove-peer -dir /tmp/trove-owner
trove-peer -dir /tmp/trove-replica
```

Note the two printed `node id:` values as `$OWNER` and `$REPLICA`.

## 2. Start the owner (send-only)

Put some files under the owner's folder root first, then:

```
trove-peer -dir /tmp/trove-owner -trove "$TROVE" -listen 0.0.0.0:22000 \
  -root /path/to/source -share demo -peer "$REPLICA" -sync-role owner
```

## 3. Start the replica (receive-only)

```
trove-peer -dir /tmp/trove-replica -trove "$TROVE" -listen 0.0.0.0:22000 \
  -root /path/to/dest -share demo -peer "$OWNER" -sync-role replica
```

## 4. Verify

- Both logs reach `session active`.
- `/path/to/dest` materializes the owner's tree bit-exact (compare with `diff -r` or
  per-file `sha256sum`), including file modes and symlink targets.
- Edit a file on the owner → within a few seconds (`announceInterval`) it propagates.
- Delete a file on the owner → it is removed on the replica.
- Rename a large file on the owner → it appears renamed on the replica with no bulk
  re-transfer (watch the owner's served-chunk count stay flat).

The replica never originates: it is `-sync-role replica` and runs no scanner over its
destination, so materialized files are never re-ingested as local edits.
