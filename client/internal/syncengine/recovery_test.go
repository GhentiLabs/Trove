package syncengine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/snapshot"
)

// TestRecoveryReaderConvergesViaEngine proves the member-source recovery path: a fresh reader
// with no roster context (nil AuthorWriter) and a plaintext staging store pulls a folder
// bit-exact from a source and signals completion through OnConverged — for an encrypted source
// folder (served plaintext over the tunnel) and an unencrypted one alike.
func TestRecoveryReaderConvergesViaEngine(t *testing.T) {
	for _, enc := range []bool{false, true} {
		name := "plaintext"
		if enc {
			name = "encrypted"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			owner := newPeer(t, ownerID)
			if enc {
				encrypt(&owner, testKey(0x33))
			}
			writeFile(t, owner.root, "a.txt", []byte("hello recovery"))
			writeFile(t, owner.root, "sub/b.bin", pseudoRandom(2<<20, 7))
			owner.scan(t)

			// The recovery reader stages plaintext and trusts the named source (nil AuthorWriter).
			rec := newPeer(t, replicaID)
			os, rs := memSessionPair(t, ctx, owner, rec)

			ownerCoord := NewCoordinator(folderID, owner.fc, owner.chunks, 0, nil)
			ownerEng, err := New(Options{Session: os, Folders: []FolderConfig{{
				FolderID: folderID, Role: RoleWriter, Root: owner.root, FolderCtx: owner.fc,
				Model: owner.model, Chunks: owner.chunks, Coord: ownerCoord,
			}}})
			if err != nil {
				t.Fatalf("owner engine: %v", err)
			}

			converged := make(chan struct{})
			var once sync.Once
			recCoord := NewCoordinator(folderID, rec.fc, rec.chunks, 0, nil)
			recEng, err := New(Options{
				Session:     rs,
				OnConverged: func(string, snapshot.Root, uint64, int64) { once.Do(func() { close(converged) }) },
				Folders: []FolderConfig{{
					FolderID: folderID, Role: RoleReader, Root: rec.root, FolderCtx: rec.fc,
					Model: rec.model, Chunks: rec.chunks, Coord: recCoord,
				}},
			})
			if err != nil {
				t.Fatalf("recovery engine: %v", err)
			}

			os.SetControlHandler(ownerEng.Handle)
			rs.SetControlHandler(recEng.Handle)
			go func() { _ = os.Run(ctx) }()
			go func() { _ = rs.Run(ctx) }()
			go func() { _ = ownerEng.Drive(ctx) }()
			go func() { _ = recEng.Drive(ctx) }()

			select {
			case <-converged:
			case <-time.After(10 * time.Second):
				t.Fatal("recovery did not converge via OnConverged")
			}
			assertTreesEqual(t, owner.root, rec.root)
		})
	}
}
