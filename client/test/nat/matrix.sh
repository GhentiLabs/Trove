#!/usr/bin/env bash
# Builds the NAT harness image once, then runs each NAT-type cell in its own
# privileged container concurrently (each builds an isolated netns topology, so
# they don't collide). Wall-clock is the slowest cell, not the sum. Exits non-zero
# if any cell's outcome differs from its expectation.
set -u
cd "$(dirname "$0")/../../.." || exit 2

echo "building trove-nat-matrix image..."
docker build -f client/test/nat/Dockerfile -t trove-nat-matrix . >/dev/null || {
	echo "image build failed"
	exit 2
}

# cell <natA> <natB> <success|fail>
cell() {
	docker run --rm --privileged \
		-e NAT_A="$1" -e NAT_B="$2" -e EXPECT="$3" \
		trove-nat-matrix 2>&1 | sed "s/^/[$1x$2] /"
	return "${PIPESTATUS[0]}"
}

# gate runs a multi-peer acceptance scenario in its own container.
gate() {
	docker run --rm --privileged \
		-e SCENARIO="$1" \
		trove-nat-matrix 2>&1 | sed "s/^/[$1] /"
	return "${PIPESTATUS[0]}"
}

echo "running NAT matrix + offline gate + bidi gate + holder gate (in parallel)..."
cell prc prc success &
p1=$!
cell prc sym fail &
p2=$!
cell sym sym fail &
p3=$!
cell prc blk fail &
p4=$!
gate offline-gate &
p5=$!
gate bidi-gate &
p6=$!
gate holder-gate &
p7=$!

rc=0
wait $p1 || rc=1
wait $p2 || rc=1
wait $p3 || rc=1
wait $p4 || rc=1
wait $p5 || rc=1
wait $p6 || rc=1
wait $p7 || rc=1

if [ $rc -eq 0 ]; then
	echo "NAT matrix: ALL CELLS PASS"
else
	echo "NAT matrix: FAILURES (see cell output above)"
fi
exit $rc
