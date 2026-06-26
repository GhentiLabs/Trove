package syncengine

import (
	"bytes"
	"context"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/snapshot"
)

// materializeSnapshot reconstructs every regular file in a retained snapshot from the
// chunk store.
func materializeSnapshot(t *testing.T, p peer, root snapshot.Root) map[string][]byte {
	t.Helper()
	ctx := context.Background()
	snap, err := p.model.GetSnapshot(ctx, root)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	out := make(map[string][]byte)
	for _, leaf := range snap.Leaves {
		if leaf.Deleted {
			continue
		}
		refs, err := p.model.ManifestChunks(ctx, leaf.ManifestID)
		if err != nil {
			t.Fatalf("ManifestChunks %q: %v", leaf.Path, err)
		}
		if len(refs) == 0 {
			continue // a directory or symlink leaf carries no chunks
		}
		var buf bytes.Buffer
		if err := p.chunks.Reassemble(ctx, p.fc, chunkIDs(refs), &buf); err != nil {
			t.Fatalf("reassemble %q from history: %v", leaf.Path, err)
		}
		out[leaf.Path] = buf.Bytes()
	}
	return out
}

// TestCutoverPreservesHistoryAndConverges is the one-way -> bidirectional cutover gate:
// a folder with retained history runs one-way, a replica is promoted to writer, both
// edit the same path during one offline window, and on reconnect (a) they converge to
// byte-identical conflict copies, while (b) the pre-existing snapshot still materializes
// bit-exact — the model change does not corrupt history.
func TestCutoverPreservesHistoryAndConverges(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	writeFile(t, owner.root, "shared.txt", []byte("historical v1"))
	writeFile(t, owner.root, "keep.txt", []byte("untouched through cutover"))
	owner.scan(t)

	histRoot, err := owner.model.CurrentRoot(context.Background())
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}
	wantHistory := map[string]string{"shared.txt": "historical v1", "keep.txt": "untouched through cutover"}

	// Round 1: one-way owner -> reader replica, converging the history.
	ctx1, cancel1 := context.WithCancel(context.Background())
	os1, rs1 := memSessionPair(t, ctx1, owner, replica)
	engineOn(t, ctx1, os1, owner, RoleWriter, nil)
	engineOn(t, ctx1, rs1, replica, RoleReader, nil)
	waitSameRoot(t, owner, replica)
	cancel1()

	// Offline window: the owner edits shared.txt, and the promoted-to-writer replica
	// edits the same path. Their version vectors diverge.
	writeFile(t, owner.root, "shared.txt", []byte("owner's offline edit"))
	owner.scan(t)
	writeFile(t, replica.root, "shared.txt", []byte("replica's offline edit"))
	replica.scan(t)

	// Round 2: both are writers now.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	os2, rs2 := memSessionPair(t, ctx2, owner, replica)
	engineOn(t, ctx2, os2, owner, RoleWriter, nil)
	engineOn(t, ctx2, rs2, replica, RoleWriter, nil)

	waitSameRoot(t, owner, replica)

	// (a) Identical conflict copies on both nodes; neither edit lost.
	assertTreesEqual(t, owner.root, replica.root)
	got := fileContents(t, owner.root)
	for _, want := range []string{"owner's offline edit", "replica's offline edit", "untouched through cutover"} {
		if !got[want] {
			t.Fatalf("missing %q after cutover; have %v", want, got)
		}
	}

	// (b) The pre-existing snapshot still materializes bit-exact on the owner.
	hist := materializeSnapshot(t, owner, histRoot)
	if len(hist) != len(wantHistory) {
		t.Fatalf("history leaf count = %d, want %d", len(hist), len(wantHistory))
	}
	for path, want := range wantHistory {
		if got := string(hist[path]); got != want {
			t.Fatalf("history %q materialized as %q, want %q", path, got, want)
		}
	}
}
