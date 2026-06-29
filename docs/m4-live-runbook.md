# M4 live convergence runbook (the integration gate)

The deterministic in-process tests are the gate CI runs. The e2e matrix
(`make e2e`) reproduces the **entire gate shape in containers over real
holepunch**: a `SCENARIO=offline-gate` run stands up an owner and two punchable
replicas, takes one replica offline while the owner edits, deletes, and renames, then
reconnects it — asserting startup repair, anti-entropy catch-up with no resurrection,
and queryable receipts for both replicas. This runbook is the **human-run ≥3-machine
version on real machines across real NATs**: the final sign-off the container harness
stands in for.

Prerequisites: `trove-peer` built on every machine (`make build`); a reachable Trove
discovery server URL `TROVE="trove://host:port?id=<fingerprint>"`.

A shared folder is a group: the owner *founds* it, *invites* each replica by node id +
public key, and each replica *joins*. Roles are derived from the group id (the founder
is the owner/writer; everyone else is a reader/replica) — there is no `-sync-role`.

## 1. Identities

On each replica, print its node id and public key (the first call mints the keypair):

```
trove-peer identity -dir /tmp/trove-r1     # → $R1_ID, $R1_KEY
trove-peer identity -dir /tmp/trove-r2     # → $R2_ID, $R2_KEY
```

## 2. Found the group and invite the replicas (owner)

Seed some files under the owner's root first, then:

```
trove-peer found  -dir /tmp/trove-owner -root /path/to/source        # → $GROUP
trove-peer invite -dir /tmp/trove-owner -group "$GROUP" -node "$R1_ID" -key "$R1_KEY"
trove-peer invite -dir /tmp/trove-owner -group "$GROUP" -node "$R2_ID" -key "$R2_KEY"
```

## 3. Join (each replica)

```
trove-peer join -dir /tmp/trove-r1 -group "$GROUP" -root /path/to/dest1
trove-peer join -dir /tmp/trove-r2 -group "$GROUP" -root /path/to/dest2
```

## 4. Run (all three)

```
trove-peer run -dir /tmp/trove-owner -trove "$TROVE" -listen 0.0.0.0:22000
trove-peer run -dir /tmp/trove-r1    -trove "$TROVE" -listen 0.0.0.0:22000
trove-peer run -dir /tmp/trove-r2    -trove "$TROVE" -listen 0.0.0.0:22000
```

Each log should reach `session active`, and both destinations should materialize the
owner's tree bit-exact (`diff -r`, or per-file `sha256sum`, including modes and symlink
targets). A replica with members it hasn't met learns the full roster via gossip
bootstrapped from the founder (derivable from the group id).

## 5. The gate: edit + delete + rename, with one replica offline

1. **Take r2 offline** (Ctrl-C its `run`). Leave the owner and r1 running.
2. On the owner, in one batch: **edit** a file's contents, **delete** another file, and
   **rename** a third (a large one). The owner's watcher picks the changes up; within a
   few seconds r1 converges:
   - the edit appears on r1;
   - the deleted file is removed on r1 (and does not come back);
   - the renamed file appears under its new name with no bulk re-transfer (the owner's
     served-chunk count barely moves — a rename ships manifests, not chunk bytes).
3. **Bring r2 back online** (`run` again). It catches up via anti-entropy from its
   cursor: it applies the edit, the deletion (no resurrection), and the rename, ending
   bit-exact with the owner — without re-pulling unchanged chunks.
4. If a synced file is deleted out-of-band under a replica's root while it is stopped,
   restarting it re-materializes the file from local chunks at startup (before peers
   attach), with no owner round-trip.

## 6. Verify the receipts ("last synced" is queryable)

On the owner, after both replicas converge:

```
trove-peer status -dir /tmp/trove-owner
```

It prints, per folder, one `peer <id>  seq=<n>  last synced <time>` line for **each**
replica that has acknowledged convergence — the owner's record that r1 and r2 reached
its current root. On each replica, `trove-peer status` shows a single line for the
owner. After step 5, every receipt's `seq` should match the owner's high-water and the
`last synced` time should be recent.

**Gate met when:** all three reach `session active`; both replicas converge bit-exact
through the edit, delete, and rename; the replica that was offline catches up correctly
on reconnect; and both ends report matching, recent receipts via `status`.
