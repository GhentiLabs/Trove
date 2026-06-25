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
	for ns in coord natA clA natB clB; do ip netns del $ns 2>/dev/null; done
	ip link del br0 2>/dev/null
	# netns/link teardown is asynchronous; wait for br0 to vanish so the next cell
	# does not race a half-deleted topology.
	for i in $(seq 1 25); do ip link show br0 >/dev/null 2>&1 || break; sleep 0.2; done
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

	ip netns exec coord env \
		TROVE_DISCOVERY_LISTEN_ADDR=0.0.0.0:${PORT} \
		TROVE_DISCOVERY_STUN_ADDR=0.0.0.0:${PORT} \
		TROVE_DISCOVERY_METRICS_ADDR=127.0.0.1:9090 \
		TROVE_DISCOVERY_SERVER_KEY=$STATE/coord.key \
		TROVE_DISCOVERY_SERVER_CERT=$STATE/coord.crt \
		TROVE_DISCOVERY_ANALYTICS_DB=$STATE/coord.db \
		discovery-server >"$LOG/coord" 2>&1 &
	local coord_pid=$!

	local fp="" i
	for i in $(seq 1 50); do
		fp=$(grep -o '"fingerprint":"[^"]*"' "$LOG/coord" 2>/dev/null | head -1 | cut -d'"' -f4)
		[ -n "$fp" ] && break
		sleep 0.2
	done
	if [ -z "$fp" ]; then
		log "coordinator failed to start"; cat "$LOG/coord"; kill $coord_pid 2>/dev/null; return 2
	fi
	local trove="trove://${COORD_IP}:${PORT}?id=${fp}"

	local idA idB
	idA=$(ip netns exec clA trove-peer -dir "$STATE/A" 2>/dev/null | awk '/node id:/{print $3}')
	idB=$(ip netns exec clB trove-peer -dir "$STATE/B" 2>/dev/null | awk '/node id:/{print $3}')

	ip netns exec clA trove-peer -dir "$STATE/A" -trove "$trove" -listen 0.0.0.0:22000 -share demo -peer "$idB" -debug >"$LOG/A" 2>&1 &
	local pa=$!
	ip netns exec clB trove-peer -dir "$STATE/B" -trove "$trove" -listen 0.0.0.0:22000 -share demo -peer "$idA" -debug >"$LOG/B" 2>&1 &
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

	kill $pa $pb $coord_pid 2>/dev/null
	wait $pa $pb 2>/dev/null

	local result="fail" took=""
	if [ $ok -eq 1 ] && [ $stable -eq 1 ]; then
		result="success"
		took=" in ${i}s"
	fi

	if [ "$result" = "$expect" ]; then
		if [ "$expect" = fail ] && { [ $attempted -eq 0 ] || [ $alive -eq 0 ]; }; then
			log "cell ${na}x${nb}: failed but not gracefully (punch attempted=$attempted, peers alive=$alive)  FAIL"
		else
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

main() {
	if ! ip netns add _probe 2>/dev/null; then
		log "this harness needs --privileged (CAP_NET_ADMIN + netns)"; exit 2
	fi
	ip netns del _probe 2>/dev/null
	trap teardown EXIT

	# One cell per invocation (cone<->cone is punchable once the STUN reflexive
	# address is correct; any symmetric side is not punchable and must fail
	# gracefully, no relay). The Makefile runs the three cells as parallel
	# containers; a bare `docker run` runs the default punchable cell.
	run_cell "$NAT_A" "$NAT_B" "$EXPECT"
}

main "$@"
