package syncengine

import (
	"context"
	"fmt"
	"testing"
)

// TestManifestDeltaPagination converges a folder whose full manifest delta exceeds a
// single control frame, so the owner must send it in multiple pages.
func TestManifestDeltaPagination(t *testing.T) {
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	for i := range 60 {
		writeFile(t, owner.root, fmt.Sprintf("f%02d.txt", i), []byte(fmt.Sprintf("contents of file %d", i)))
	}
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerSess, replicaSess := memSessionPair(t, ctx, owner, replica)

	// A tiny delta budget forces many small pages out of the 60 manifests.
	ownerEng, err := New(Options{
		Session: ownerSess, MaxDeltaBytes: 512,
		Folders: []FolderConfig{{FolderID: folderID, Role: RoleOwner, Root: owner.root, Model: owner.model, Chunks: owner.chunks}},
	})
	if err != nil {
		t.Fatalf("owner engine: %v", err)
	}
	coord := NewCoordinator(folderID, replica.fc, replica.chunks, 0, nil)
	replicaEng, err := New(Options{
		Session: replicaSess,
		Folders: []FolderConfig{{FolderID: folderID, Role: RoleReplica, Root: replica.root, Model: replica.model, Chunks: replica.chunks, Coord: coord}},
	})
	if err != nil {
		t.Fatalf("replica engine: %v", err)
	}
	ownerSess.SetControlHandler(ownerEng.Handle)
	replicaSess.SetControlHandler(replicaEng.Handle)
	go func() { _ = ownerSess.Run(ctx) }()
	go func() { _ = replicaSess.Run(ctx) }()
	go func() { _ = ownerEng.Drive(ctx) }()
	go func() { _ = replicaEng.Drive(ctx) }()

	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
	if n := ownerEng.DeltasSent(); n < 2 {
		t.Fatalf("DeltasSent = %d, want multiple pages", n)
	}
}
