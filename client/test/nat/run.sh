#!/usr/bin/env bash
# NAT hole-punch matrix harness. Builds a netns topology — two peers, each behind
# its own NAT router, plus a coordinator (the real discovery server) on a shared
# "internet" bridge — and asserts, per NAT-type cell, that the peers either reach
# an Active session (punchable) or fail gracefully (not punchable, no relay).
#
# Punchability is decided by NAT *mapping* behaviour, so the cells exercise that axis
# rather than every textbook NAT name:
#   prc  port-restricted cone   = plain masquerade        (endpoint-independent mapping)
#   sym  symmetric              = masquerade fully-random  (address-and-port-dependent mapping)
#   blk  UDP-blocked firewall   = masquerade + drop UDP    (TCP discovery works, no UDP punch)
# A cone NAT keeps one external mapping per internal socket regardless of peer, so the
# STUN-observed address is the one the peer punches toward — punchable. port-restricted
# is the strictest cone, so full-cone and address-restricted (more permissive) succeed a
# fortiori and need no separate cell. Symmetric picks a fresh port per destination, so the
# observed address is wrong by the time the peer dials — unpunchable. UDP-blocked is the
# third real unpunchable class: discovery/signalling still work over TCP, but no UDP punch
# can land. Both unpunchable classes must fail gracefully (no relay).
#
# Run with --privileged. Exits non-zero if any cell's outcome differs from expected.
set -u

COORD_IP=100.64.0.2
PORT=8443
STATE=/run/trove/state
LOG=/run/trove/log
# A punchable pair connects within a handful of retry cycles; a non-punchable pair
# never does, so the fail-expected cell uses a shorter window than the success one.
WAIT_OK="${WAIT_OK:-60}"
WAIT_FAIL="${WAIT_FAIL:-20}"

# One cell per container invocation, so the Makefile can run the matrix in parallel.
NAT_A="${NAT_A:-prc}"
NAT_B="${NAT_B:-prc}"
EXPECT="${EXPECT:-success}"

log() { echo "[harness] $*"; }

setup_base() {
	ip link add br0 type bridge
	ip addr add 100.64.0.1/24 dev br0
	ip link set br0 up

	ip netns add coord
	ip link add co-c type veth peer name co-b
	ip link set co-c netns coord
	ip link set co-b master br0 up
	ip netns exec coord ip link set lo up
	ip netns exec coord ip addr add ${COORD_IP}/24 dev co-c
	ip netns exec coord ip link set co-c up
	ip netns exec coord ip route add default via 100.64.0.1
}

# setup_side <A|B> <lan-prefix> <wan-ip>
setup_side() {
	local s=$1 lan=$2 wan=$3
	ip netns add nat$s
	ip netns add cl$s
	ip netns exec nat$s ip link set lo up
	ip netns exec cl$s ip link set lo up

	ip link add lan$s-c type veth peer name lan$s-r
	ip link set lan$s-c netns cl$s
	ip link set lan$s-r netns nat$s
	ip netns exec cl$s ip addr add ${lan}.2/24 dev lan$s-c
	ip netns exec nat$s ip addr add ${lan}.1/24 dev lan$s-r
	ip netns exec cl$s ip link set lan$s-c up
	ip netns exec nat$s ip link set lan$s-r up
	ip netns exec cl$s ip route add default via ${lan}.1

	ip link add wan$s-r type veth peer name wan$s-b
	ip link set wan$s-r netns nat$s
	ip link set wan$s-b master br0 up
	ip netns exec nat$s ip addr add ${wan}/24 dev wan$s-r
	ip netns exec nat$s ip link set wan$s-r up
	ip netns exec nat$s ip route add default via 100.64.0.1
	ip netns exec nat$s sysctl -q -w net.ipv4.ip_forward=1
	# Real WAN latency. Without it the link is sub-millisecond, which makes the
	# simultaneous-open window unrealistically tiny — real NATs see 10-50ms RTT, so
	# both peers' outbound packets cross before the other's arrives. ${WAN_DELAY}.
	ip netns exec nat$s tc qdisc add dev wan$s-r root netem delay "${WAN_DELAY:-15ms}" 3ms distribution normal
}

# apply_nat <A|B> <lan-prefix> <prc|sym|blk>
apply_nat() {
	local s=$1 lan=$2 type=$3 mode="masquerade"
	[ "$type" = sym ] && mode="masquerade fully-random"
	ip netns exec nat$s nft -f - <<EOF
table ip nat {
	chain post {
		type nat hook postrouting priority 100;
		ip saddr ${lan}.0/24 oifname "wan$s-r" $mode
	}
}
EOF
	# A UDP-blocking firewall: TCP (discovery announce/lookup, WSS signalling) still
	# traverses, but every forwarded UDP datagram is dropped, so STUN and the QUIC
	# punch cannot land. Models a TCP-443-only corporate network.
	if [ "$type" = blk ]; then
		ip netns exec nat$s nft -f - <<EOF
table ip filter {
	chain forward {
		type filter hook forward priority 0;
		meta l4proto udp drop
	}
}
EOF
	fi
}

teardown() {
	local ns i
	for ns in coord natA clA natB clB natC clC; do ip netns del $ns 2>/dev/null; done
	ip link del br0 2>/dev/null
	# netns/link teardown is asynchronous; wait for br0 to vanish so the next cell
	# does not race a half-deleted topology.
	for i in $(seq 1 25); do ip link show br0 >/dev/null 2>&1 || break; sleep 0.2; done
}

# start_coordinator launches the discovery server in the coord netns and sets COORD_PID
# and TROVE_URL (the trove:// url with the server's mTLS fingerprint).
start_coordinator() {
	ip netns exec coord env \
		TROVE_DISCOVERY_LISTEN_ADDR=0.0.0.0:${PORT} \
		TROVE_DISCOVERY_STUN_ADDR=0.0.0.0:${PORT} \
		TROVE_DISCOVERY_METRICS_ADDR=127.0.0.1:9090 \
		TROVE_DISCOVERY_SERVER_KEY=$STATE/coord.key \
		TROVE_DISCOVERY_SERVER_CERT=$STATE/coord.crt \
		TROVE_DISCOVERY_ANALYTICS_DB=$STATE/coord.db \
		discovery-server >"$LOG/coord" 2>&1 &
	COORD_PID=$!
	local fp="" i
	for i in $(seq 1 50); do
		fp=$(grep -o '"fingerprint":"[^"]*"' "$LOG/coord" 2>/dev/null | head -1 | cut -d'"' -f4)
		[ -n "$fp" ] && break
		sleep 0.2
	done
	if [ -z "$fp" ]; then
		log "coordinator failed to start"; cat "$LOG/coord"; kill $COORD_PID 2>/dev/null; return 2
	fi
	TROVE_URL="trove://${COORD_IP}:${PORT}?id=${fp}"
}

# run_cell <natA-type> <natB-type> <success|fail>
run_cell() {
	local na=$1 nb=$2 expect=$3
	teardown
	rm -rf "$STATE" "$LOG"
	mkdir -p "$STATE" "$LOG"

	setup_base
	setup_side A 192.168.10 100.64.0.10
	setup_side B 192.168.20 100.64.0.20
	apply_nat A 192.168.10 "$na"
	apply_nat B 192.168.20 "$nb"

	if ! start_coordinator; then return 2; fi
	local coord_pid=$COORD_PID trove=$TROVE_URL

	# Identities (node id + public key); the first call mints each keypair on disk.
	local idB keyB
	idB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/node id:/{print $3}')
	keyB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/public key:/{print $3}')

	# A owns a folder group: seed deterministic content, found the group, invite B as a
	# reader, B joins into an empty root. Local config steps, no network.
	local SRC="$STATE/A/share" DST="$STATE/B/share" group
	mkdir -p "$SRC/sub" "$DST"
	head -c 1048576 /dev/urandom >"$SRC/big.bin"
	head -c 262144 /dev/urandom >"$SRC/movable.bin"
	printf 'hello trove\n' >"$SRC/sub/note.txt"
	group=$(ip netns exec clA trove-peer found -dir "$STATE/A" -root "$SRC" | awk '/group id:/{print $3}')
	if [ -z "$group" ]; then log "found failed"; kill $coord_pid 2>/dev/null; return 2; fi
	ip netns exec clA trove-peer invite -dir "$STATE/A" -group "$group" -node "$idB" -key "$keyB" >"$LOG/setup" 2>&1
	ip netns exec clB trove-peer join -dir "$STATE/B" -group "$group" -root "$DST" >>"$LOG/setup" 2>&1

	ip netns exec clA trove-peer run -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/A" 2>&1 &
	local pa=$!
	ip netns exec clB trove-peer run -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/B" 2>&1 &
	local pb=$!

	local wait=$WAIT_OK
	[ "$expect" = fail ] && wait=$WAIT_FAIL
	local ok=0
	for i in $(seq 1 "$wait"); do
		if grep -q "session active" "$LOG/A" && grep -q "session active" "$LOG/B"; then ok=1; break; fi
		if ! kill -0 $pa 2>/dev/null || ! kill -0 $pb 2>/dev/null; then break; fi
		sleep 1
	done

	# Two qualities beyond "did a session form":
	#   success must be STABLE — a punch that forms then immediately flaps is not a
	#   real connection, so both peers must survive a short post-Active window.
	#   failure must be GRACEFUL — the unpunchable pairs must have actively attempted
	#   the punch and still be running (no crash, no hang); Trove never relays.
	local stable=1 attempted=0 alive=1
	if [ $ok -eq 1 ]; then
		sleep 3
		kill -0 $pa 2>/dev/null && kill -0 $pb 2>/dev/null || stable=0
	fi
	grep -q "holepunch" "$LOG/A" && grep -q "holepunch" "$LOG/B" && attempted=1
	kill -0 $pa 2>/dev/null && kill -0 $pb 2>/dev/null || alive=0

	# On a punchable pair the replica must receive the owner's folder BIT-EXACT — this
	# is the real end-to-end check: membership auth + gossip bootstrap + the sync engine.
	local xfer="n/a" j
	if [ $ok -eq 1 ] && [ $stable -eq 1 ]; then
		xfer="fail"
		for j in $(seq 1 30); do
			if cmp -s "$SRC/big.bin" "$DST/big.bin" 2>/dev/null \
			   && cmp -s "$SRC/movable.bin" "$DST/movable.bin" 2>/dev/null \
			   && cmp -s "$SRC/sub/note.txt" "$DST/sub/note.txt" 2>/dev/null; then
				xfer="ok"; break
			fi
			sleep 1
		done
	fi

	# Second round over the live holepunched path: the owner edits a file, deletes
	# another, and renames a third; the replica must converge to the new state —
	# deletion does not resurrect and a rename moves no chunk data. Then the owner's
	# receipt for the replica must be queryable ("last synced"). This is the Phase D
	# convergence shape (edit + delete + rename) the M4 gate requires.
	local round2="n/a" receipt="n/a"
	if [ "$xfer" = ok ]; then
		round2="fail"
		printf 'edited after first sync\n' >"$SRC/sub/note.txt"
		rm -f "$SRC/big.bin"
		mv "$SRC/movable.bin" "$SRC/moved.bin"
		for j in $(seq 1 30); do
			if cmp -s "$SRC/sub/note.txt" "$DST/sub/note.txt" 2>/dev/null \
			   && cmp -s "$SRC/moved.bin" "$DST/moved.bin" 2>/dev/null \
			   && [ ! -e "$DST/big.bin" ] && [ ! -e "$DST/movable.bin" ]; then
				round2="ok"; break
			fi
			sleep 1
		done
		receipt="fail"
		for j in $(seq 1 15); do
			if ip netns exec clA trove-peer status -dir "$STATE/A" 2>/dev/null | grep -q "peer ${idB}"; then
				receipt="ok"; break
			fi
			sleep 1
		done
	fi

	kill $pa $pb $coord_pid 2>/dev/null
	wait $pa $pb 2>/dev/null

	local result="fail" took=""
	if [ $ok -eq 1 ] && [ $stable -eq 1 ] && [ "$xfer" = ok ] && [ "$round2" = ok ] && [ "$receipt" = ok ]; then
		result="success"
		took=" in ${i}s, bit-exact + edit/delete/rename converged, receipt queryable"
	elif [ $ok -eq 1 ] && { [ "$xfer" = fail ] || [ "$round2" = fail ] || [ "$receipt" = fail ]; }; then
		log "cell ${na}x${nb}: session formed but sync FAILED (xfer=$xfer round2=$round2 receipt=$receipt)"
		log "---- A sync ----"; grep -iE "sync|manifest|chunk|scan|folder|receipt|tombstone" "$LOG/A" | tail -20
		log "---- B sync ----"; grep -iE "sync|manifest|chunk|coordinator|apply|folder|receipt" "$LOG/B" | tail -20
	fi

	if [ "$result" = "$expect" ]; then
		if [ "$expect" = fail ] && { [ $attempted -eq 0 ] || [ $alive -eq 0 ]; }; then
			log "cell ${na}x${nb}: failed but not gracefully (punch attempted=$attempted, peers alive=$alive)  FAIL"
		else
			if [ "$result" = success ]; then
				log "---- how A connected ----"; grep -E "reflexive|holepunch|connected|session active" "$LOG/A" | tail -8
				log "---- how B connected ----"; grep -E "reflexive|holepunch|connected|session active" "$LOG/B" | tail -8
			fi
			log "cell ${na}x${nb}: ${result}${took} (expected ${expect}, graceful)  PASS"
			return 0
		fi
	else
		log "cell ${na}x${nb}: ${result} (expected ${expect})  FAIL"
	fi
	log "---- peer A reflexive/connect ----"; grep -E "reflexive|connected|holepunch|dial failed" "$LOG/A" | tail -12
	log "---- peer B reflexive/connect ----"; grep -E "reflexive|connected|holepunch|dial failed" "$LOG/B" | tail -12
	log "---- natA udp conntrack (from 192.168.10.2) ----"; ip netns exec natA conntrack -L -p udp 2>/dev/null | grep -E "192.168.10.2|100.64" | head
	log "---- natB udp conntrack (from 192.168.20.2) ----"; ip netns exec natB conntrack -L -p udp 2>/dev/null | grep -E "192.168.20.2|100.64" | head
	return 1
}

report_gate_logs() {
	local p
	for p in A B C; do
		log "---- peer $p ----"
		grep -iE "session active|holepunch|reflexive|sync|manifest|chunk|apply|repair|receipt|tombstone|error" "$LOG/$p" 2>/dev/null | tail -15
	done
}

# run_offline_gate runs the M4 Phase D acceptance shape over real holepunch: an owner (A)
# and two punchable replicas (B, C) converge a folder; then C goes offline while the owner
# edits, deletes, and renames files; B converges live; C reconnects and both catches up via
# anti-entropy and re-materializes a file deleted out-of-band under it at startup — ending
# bit-exact with no resurrected deletion, and the owner holds a receipt for both replicas.
run_offline_gate() {
	teardown
	rm -rf "$STATE" "$LOG"
	mkdir -p "$STATE" "$LOG"

	setup_base
	setup_side A 192.168.10 100.64.0.10
	setup_side B 192.168.20 100.64.0.20
	setup_side C 192.168.30 100.64.0.30
	apply_nat A 192.168.10 prc
	apply_nat B 192.168.20 prc
	apply_nat C 192.168.30 prc

	if ! start_coordinator; then return 2; fi
	local trove=$TROVE_URL j

	local idB keyB idC keyC
	idB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/node id:/{print $3}')
	keyB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/public key:/{print $3}')
	idC=$(ip netns exec clC trove-peer identity -dir "$STATE/C" | awk '/node id:/{print $3}')
	keyC=$(ip netns exec clC trove-peer identity -dir "$STATE/C" | awk '/public key:/{print $3}')

	local SRC="$STATE/A/share" DSTB="$STATE/B/share" DSTC="$STATE/C/share" group
	mkdir -p "$SRC/sub" "$DSTB" "$DSTC"
	head -c 1048576 /dev/urandom >"$SRC/big.bin"
	head -c 262144 /dev/urandom >"$SRC/movable.bin"
	printf 'hello trove\n' >"$SRC/sub/note.txt"
	printf 'never touched by the owner\n' >"$SRC/stable.txt"
	group=$(ip netns exec clA trove-peer found -dir "$STATE/A" -root "$SRC" | awk '/group id:/{print $3}')
	if [ -z "$group" ]; then log "offline-gate: found failed"; kill $COORD_PID 2>/dev/null; return 2; fi
	ip netns exec clA trove-peer invite -dir "$STATE/A" -group "$group" -node "$idB" -key "$keyB" >"$LOG/setup" 2>&1
	ip netns exec clA trove-peer invite -dir "$STATE/A" -group "$group" -node "$idC" -key "$keyC" >>"$LOG/setup" 2>&1
	ip netns exec clB trove-peer join -dir "$STATE/B" -group "$group" -root "$DSTB" >>"$LOG/setup" 2>&1
	ip netns exec clC trove-peer join -dir "$STATE/C" -group "$group" -root "$DSTC" >>"$LOG/setup" 2>&1

	ip netns exec clA trove-peer run -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/A" 2>&1 &
	local pa=$!
	ip netns exec clB trove-peer run -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/B" 2>&1 &
	local pb=$!
	ip netns exec clC trove-peer run -dir "$STATE/C" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/C" 2>&1 &
	local pc=$!

	# Round 1: both replicas receive the founding folder bit-exact.
	local r1="fail"
	for j in $(seq 1 90); do
		if cmp -s "$SRC/big.bin" "$DSTB/big.bin" 2>/dev/null && cmp -s "$SRC/stable.txt" "$DSTB/stable.txt" 2>/dev/null \
		   && cmp -s "$SRC/big.bin" "$DSTC/big.bin" 2>/dev/null && cmp -s "$SRC/stable.txt" "$DSTC/stable.txt" 2>/dev/null; then
			r1="ok"; break
		fi
		sleep 1
	done
	if [ "$r1" != ok ]; then
		log "offline-gate: round 1 convergence FAILED"; report_gate_logs
		kill $pa $pb $pc $COORD_PID 2>/dev/null; return 1
	fi

	# C goes offline. Delete a synced, owner-stable file under C out-of-band: only the
	# replica's startup repair can restore it (the owner never re-sends it), so its
	# presence after restart proves D3 in the live setting.
	kill $pc 2>/dev/null; wait $pc 2>/dev/null
	rm -f "$DSTC/stable.txt"

	# Owner mutates while C is offline: edit + delete + rename.
	printf 'edited while C was offline\n' >"$SRC/sub/note.txt"
	rm -f "$SRC/big.bin"
	mv "$SRC/movable.bin" "$SRC/moved.bin"

	# B converges live to the new state.
	local r2b="fail"
	for j in $(seq 1 60); do
		if cmp -s "$SRC/sub/note.txt" "$DSTB/sub/note.txt" 2>/dev/null && cmp -s "$SRC/moved.bin" "$DSTB/moved.bin" 2>/dev/null \
		   && [ ! -e "$DSTB/big.bin" ] && [ ! -e "$DSTB/movable.bin" ]; then
			r2b="ok"; break
		fi
		sleep 1
	done

	# C reconnects: startup repair restores stable.txt, then anti-entropy applies the
	# edit, the deletion (no resurrection), and the rename.
	ip netns exec clC trove-peer run -dir "$STATE/C" -trove "$trove" -listen 0.0.0.0:22000 -debug >>"$LOG/C" 2>&1 &
	pc=$!
	local r2c="fail"
	for j in $(seq 1 90); do
		if cmp -s "$SRC/sub/note.txt" "$DSTC/sub/note.txt" 2>/dev/null && cmp -s "$SRC/moved.bin" "$DSTC/moved.bin" 2>/dev/null \
		   && cmp -s "$SRC/stable.txt" "$DSTC/stable.txt" 2>/dev/null \
		   && [ ! -e "$DSTC/big.bin" ] && [ ! -e "$DSTC/movable.bin" ]; then
			r2c="ok"; break
		fi
		sleep 1
	done
	local repair="fail"
	grep -q "repaired files from local chunks" "$LOG/C" && repair="ok"

	# Both replicas' receipts are queryable on the owner.
	local receipts="fail" out
	for j in $(seq 1 20); do
		out=$(ip netns exec clA trove-peer status -dir "$STATE/A" 2>/dev/null)
		if echo "$out" | grep -q "peer ${idB}" && echo "$out" | grep -q "peer ${idC}"; then
			receipts="ok"; break
		fi
		sleep 1
	done

	kill $pa $pb $pc $COORD_PID 2>/dev/null
	wait $pa $pb $pc 2>/dev/null

	if [ "$r1" = ok ] && [ "$r2b" = ok ] && [ "$r2c" = ok ] && [ "$repair" = ok ] && [ "$receipts" = ok ]; then
		log "offline-gate: PASS (3 peers; B live + C offline→repair+catch-up through edit/delete/rename; receipts for B and C)"
		return 0
	fi
	log "offline-gate: FAIL (r1=$r1 r2b=$r2b r2c=$r2c repair=$repair receipts=$receipts)"
	report_gate_logs
	return 1
}

# run_bidi_gate is the M5 acceptance shape over real holepunch: two writers converge a
# folder, both edit the SAME path during one offline window, and on reconnect converge to
# byte-identical trees holding the deterministic winner plus the loser as a conflict copy —
# neither edit lost.
run_bidi_gate() {
	teardown
	rm -rf "$STATE" "$LOG"
	mkdir -p "$STATE" "$LOG"

	setup_base
	setup_side A 192.168.10 100.64.0.10
	setup_side B 192.168.20 100.64.0.20
	apply_nat A 192.168.10 prc
	apply_nat B 192.168.20 prc

	if ! start_coordinator; then return 2; fi
	local trove=$TROVE_URL j

	local idB keyB
	idB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/node id:/{print $3}')
	keyB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/public key:/{print $3}')

	local SRC="$STATE/A/share" DSTB="$STATE/B/share" group
	mkdir -p "$SRC" "$DSTB"
	printf 'founding content\n' >"$SRC/doc.txt"
	group=$(ip netns exec clA trove-peer found -dir "$STATE/A" -root "$SRC" | awk '/group id:/{print $3}')
	if [ -z "$group" ]; then log "bidi-gate: found failed"; kill $COORD_PID 2>/dev/null; return 2; fi
	# B is admitted as a WRITER (two-way), not a reader.
	ip netns exec clA trove-peer invite -dir "$STATE/A" -group "$group" -node "$idB" -key "$keyB" -writer >"$LOG/setup" 2>&1
	ip netns exec clB trove-peer join -dir "$STATE/B" -group "$group" -root "$DSTB" >>"$LOG/setup" 2>&1

	ip netns exec clA trove-peer run -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/A" 2>&1 &
	local pa=$!
	ip netns exec clB trove-peer run -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/B" 2>&1 &
	local pb=$!

	# Round 1: B converges the founding state.
	local r1="fail"
	for j in $(seq 1 90); do
		if cmp -s "$SRC/doc.txt" "$DSTB/doc.txt" 2>/dev/null; then r1="ok"; break; fi
		sleep 1
	done
	if [ "$r1" != ok ]; then
		log "bidi-gate: round 1 convergence FAILED"; report_gate_logs
		kill $pa $pb $COORD_PID 2>/dev/null; return 1
	fi

	# Both go offline and edit the same path differently — a true concurrent conflict.
	kill $pa $pb 2>/dev/null; wait $pa $pb 2>/dev/null
	printf 'edit by writer A\n' >"$SRC/doc.txt"
	printf 'edit by writer B\n' >"$DSTB/doc.txt"

	ip netns exec clA trove-peer run -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -debug >>"$LOG/A" 2>&1 &
	pa=$!
	ip netns exec clB trove-peer run -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -debug >>"$LOG/B" 2>&1 &
	pb=$!

	# Converge: A and B end byte-identical (winner at doc.txt + one conflict copy), and both
	# edits survive somewhere in the tree.
	local conv="fail"
	for j in $(seq 1 90); do
		if diff -rq -x '.trove-tmp*' "$SRC" "$DSTB" >/dev/null 2>&1 \
		   && grep -rqsF 'edit by writer A' "$SRC" && grep -rqsF 'edit by writer B' "$SRC" \
		   && [ "$(find "$SRC" -name 'doc.conflict-*' | wc -l)" -eq 1 ]; then
			conv="ok"; break
		fi
		sleep 1
	done

	kill $pa $pb $COORD_PID 2>/dev/null
	wait $pa $pb 2>/dev/null

	if [ "$conv" = ok ]; then
		log "bidi-gate: PASS (2 writers; concurrent same-path offline edits → byte-identical keep-both convergence over holepunch)"
		return 0
	fi
	log "bidi-gate: FAIL (conv=$conv)"
	report_gate_logs
	return 1
}

# run_holder_gate is the M6 acceptance shape over real holepunch: a writer (A) pushes an
# encrypted folder to an untrusted holder (B) that never receives the key; the writer then goes
# offline and a fresh, non-member machine (C) restores the folder bit-exact from the holder
# alone, using only the recovery code and proving key knowledge via the folder verifier. The
# holder stores only sealed ciphertext under blinded ids — no plaintext name or content leaks.
run_holder_gate() {
	teardown
	rm -rf "$STATE" "$LOG"
	mkdir -p "$STATE" "$LOG"

	setup_base
	setup_side A 192.168.10 100.64.0.10
	setup_side B 192.168.20 100.64.0.20
	setup_side C 192.168.30 100.64.0.30
	apply_nat A 192.168.10 prc
	apply_nat B 192.168.20 prc
	apply_nat C 192.168.30 prc

	if ! start_coordinator; then return 2; fi
	local trove=$TROVE_URL j

	local idB keyB
	idB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/node id:/{print $3}')
	keyB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/public key:/{print $3}')

	local SRC="$STATE/A/share" DSTC="$STATE/C/restored" group code
	mkdir -p "$SRC/sub" "$DSTC"
	head -c 1048576 /dev/urandom >"$SRC/big.bin"
	printf 'TOP-SECRET-PLAINTEXT-MARKER\n' >"$SRC/secret.txt"
	printf 'hello holder\n' >"$SRC/sub/note.txt"

	ip netns exec clA trove-peer found -dir "$STATE/A" -root "$SRC" -encrypted >"$LOG/found" 2>&1
	group=$(awk '/group id:/{print $3}' "$LOG/found")
	code=$(awk '/recovery code:/{print $3}' "$LOG/found")
	if [ -z "$group" ] || [ -z "$code" ]; then
		log "holder-gate: found -encrypted failed"; cat "$LOG/found"
		kill $COORD_PID 2>/dev/null; return 2
	fi

	# B is admitted as an untrusted holder (ciphertext only, no key); C is never invited.
	ip netns exec clA trove-peer invite -dir "$STATE/A" -group "$group" -node "$idB" -key "$keyB" -holder >"$LOG/setup" 2>&1
	ip netns exec clB trove-peer join -dir "$STATE/B" -group "$group" -holder -encrypted >>"$LOG/setup" 2>&1

	ip netns exec clA trove-peer run -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/A" 2>&1 &
	local pa=$!
	ip netns exec clB trove-peer run -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/B" 2>&1 &
	local pb=$!

	# The writer pushes the sealed folder to the holder: chunks first, then the catalog, then
	# the pointer last. Wait until a large blob (big.bin's chunk) has landed AND the blob count
	# has stopped growing, so the whole set — including the pointer the restore reads first — is
	# present before the writer goes offline.
	local pushed="fail" count prev=-1 stable=0 big
	for j in $(seq 1 120); do
		count=$(find "$STATE/B/folders" -path '*/holder/*' -type f 2>/dev/null | wc -l)
		big=$(find "$STATE/B/folders" -path '*/holder/*' -type f -size +4k 2>/dev/null | wc -l)
		if [ "$big" -ge 1 ] && [ "$count" = "$prev" ]; then
			stable=$((stable + 1))
			if [ "$stable" -ge 5 ]; then pushed="ok"; break; fi
		else
			stable=0
		fi
		prev=$count
		sleep 1
	done
	if [ "$pushed" != ok ]; then
		log "holder-gate: writer never pushed blobs to the holder"; report_gate_logs
		kill $pa $pb $COORD_PID 2>/dev/null; return 1
	fi

	# The writer goes offline: the holder must serve recovery with no member online.
	kill $pa 2>/dev/null; wait $pa 2>/dev/null

	# A fresh, non-member machine restores from the holder using only the recovery code.
	ip netns exec clC trove-peer restore -dir "$STATE/C" -trove "$trove" -group "$group" -code "$code" -from "$idB" -root "$DSTC" -debug >"$LOG/C" 2>&1 &
	local prc=$!
	local restored="fail"
	for j in $(seq 1 90); do
		if cmp -s "$SRC/big.bin" "$DSTC/big.bin" 2>/dev/null && cmp -s "$SRC/secret.txt" "$DSTC/secret.txt" 2>/dev/null \
		   && cmp -s "$SRC/sub/note.txt" "$DSTC/sub/note.txt" 2>/dev/null; then
			restored="ok"; break
		fi
		kill -0 $prc 2>/dev/null || break
		sleep 1
	done

	# The holder authorized the restore through the verifier gate.
	local gated="fail"
	grep -q "holder restore authorized" "$LOG/B" && gated="ok"

	# The holder stored only ciphertext + blinded ids: no plaintext name or content is observable.
	local leak="none"
	grep -rqsF 'TOP-SECRET-PLAINTEXT-MARKER' "$STATE/B/folders" 2>/dev/null && leak="content"
	find "$STATE/B/folders" -name 'secret.txt' 2>/dev/null | grep -q . && leak="name"

	kill $pb $prc $COORD_PID 2>/dev/null
	wait $pb 2>/dev/null

	if [ "$restored" = ok ] && [ "$gated" = ok ] && [ "$leak" = none ]; then
		log "holder-gate: PASS (writer→holder ciphertext push; non-member restores bit-exact from the holder alone via verifier proof; holder never keyed, no plaintext observable)"
		return 0
	fi
	log "holder-gate: FAIL (restored=$restored gated=$gated leak=$leak)"
	report_gate_logs
	return 1
}

# run_recovery_gate <enc|plain> exercises universal recovery from a MEMBER over real holepunch: a
# writer (A) founds a folder, a reader (B) joins and converges, the writer goes offline, and a
# fresh non-member (C) recovers the folder bit-exact from the reader via the sync engine. For an
# unencrypted folder this also exercises the recovery-secret propagation A->B (B is not the founder
# but must learn the secret to serve recovery).
run_recovery_gate() {
	local mode=$1 encflag=""
	[ "$mode" = enc ] && encflag="-encrypted"
	teardown
	rm -rf "$STATE" "$LOG"
	mkdir -p "$STATE" "$LOG"

	setup_base
	setup_side A 192.168.10 100.64.0.10
	setup_side B 192.168.20 100.64.0.20
	setup_side C 192.168.30 100.64.0.30
	apply_nat A 192.168.10 prc
	apply_nat B 192.168.20 prc
	apply_nat C 192.168.30 prc

	if ! start_coordinator; then return 2; fi
	local trove=$TROVE_URL j

	local idB keyB
	idB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/node id:/{print $3}')
	keyB=$(ip netns exec clB trove-peer identity -dir "$STATE/B" | awk '/public key:/{print $3}')

	local SRC="$STATE/A/share" DSTB="$STATE/B/share" DSTC="$STATE/C/restored" group code
	mkdir -p "$SRC/sub" "$DSTB" "$DSTC"
	head -c 1048576 /dev/urandom >"$SRC/big.bin"
	printf 'recover me\n' >"$SRC/sub/note.txt"

	ip netns exec clA trove-peer found -dir "$STATE/A" -root "$SRC" $encflag >"$LOG/found" 2>&1
	group=$(awk '/group id:/{print $3}' "$LOG/found")
	code=$(awk '/recovery code:/{print $3}' "$LOG/found")
	if [ -z "$group" ] || [ -z "$code" ]; then
		log "recovery-gate($mode): found failed"; cat "$LOG/found"
		kill $COORD_PID 2>/dev/null; return 2
	fi

	ip netns exec clA trove-peer invite -dir "$STATE/A" -group "$group" -node "$idB" -key "$keyB" >"$LOG/setup" 2>&1
	ip netns exec clB trove-peer join -dir "$STATE/B" -group "$group" -root "$DSTB" $encflag >>"$LOG/setup" 2>&1

	ip netns exec clA trove-peer run -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/A" 2>&1 &
	local pa=$!
	ip netns exec clB trove-peer run -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -debug >"$LOG/B" 2>&1 &
	local pb=$!

	# The reader converges the folder from the writer (and, when unencrypted, receives the secret).
	local synced="fail"
	for j in $(seq 1 90); do
		if cmp -s "$SRC/big.bin" "$DSTB/big.bin" 2>/dev/null && cmp -s "$SRC/sub/note.txt" "$DSTB/sub/note.txt" 2>/dev/null; then
			synced="ok"; break
		fi
		sleep 1
	done
	if [ "$synced" != ok ]; then
		log "recovery-gate($mode): reader never converged"; report_gate_logs
		kill $pa $pb $COORD_PID 2>/dev/null; return 1
	fi

	# The writer goes offline: recovery must work from the reader alone.
	kill $pa 2>/dev/null; wait $pa 2>/dev/null

	ip netns exec clC trove-peer restore -dir "$STATE/C" -trove "$trove" -group "$group" -code "$code" -from "$idB" -root "$DSTC" -debug >"$LOG/C" 2>&1 &
	local prc=$!
	local restored="fail"
	for j in $(seq 1 90); do
		if cmp -s "$SRC/big.bin" "$DSTC/big.bin" 2>/dev/null && cmp -s "$SRC/sub/note.txt" "$DSTC/sub/note.txt" 2>/dev/null; then
			restored="ok"; break
		fi
		kill -0 $prc 2>/dev/null || break
		sleep 1
	done

	# The reader authorized the recovery via the verifier (member path, not holder).
	local gated="fail"
	grep -q "recovery access authorized" "$LOG/B" && gated="ok"

	kill $pb $prc $COORD_PID 2>/dev/null
	wait $pb 2>/dev/null

	if [ "$restored" = ok ] && [ "$gated" = ok ]; then
		log "recovery-gate($mode): PASS (non-member recovers bit-exact from a reader via the engine; writer offline)"
		return 0
	fi
	log "recovery-gate($mode): FAIL (restored=$restored gated=$gated)"
	report_gate_logs
	return 1
}

main() {
	if ! ip netns add _probe 2>/dev/null; then
		log "this harness needs --privileged (CAP_NET_ADMIN + netns)"; exit 2
	fi
	ip netns del _probe 2>/dev/null
	trap teardown EXIT

	case "${SCENARIO:-cell}" in
	cell)
		# One cell per invocation (cone<->cone is punchable once the STUN reflexive
		# address is correct; any symmetric side is not punchable and must fail
		# gracefully, no relay). The Makefile runs the cells as parallel containers; a
		# bare `docker run` runs the default punchable cell.
		run_cell "$NAT_A" "$NAT_B" "$EXPECT" ;;
	offline-gate)
		run_offline_gate ;;
	bidi-gate)
		run_bidi_gate ;;
	holder-gate)
		run_holder_gate ;;
	member-recovery-gate)
		run_recovery_gate enc ;;
	unencrypted-recovery-gate)
		run_recovery_gate plain ;;
	*)
		log "unknown SCENARIO ${SCENARIO}"; exit 2 ;;
	esac
}

main "$@"
