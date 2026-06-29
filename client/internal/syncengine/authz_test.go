package syncengine

import (
	"context"
	"errors"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/model"
)

var errTestAuthLookup = errors.New("author lookup failed")

// assertRejected waits until the replica has processed the owner's manifest, then asserts
// nothing materialized. The anchor is the LocalSync receipt: apply runs the author filter
// before ApplyRemote and markConverged writes the receipt only after apply returns, so a
// receipt guarantees the filter ran to completion for this reconcile — making the absence
// check exact rather than a timing guess.
func assertRejected(t *testing.T, replica peer, what string) {
	t.Helper()
	waitFor(t, convergeTimeout, "replica to process the manifest", func() bool {
		_, ok, err := replica.model.Receipt(context.Background(), model.LocalSync, ownerID)
		if err != nil {
			t.Fatalf("Receipt: %v", err)
		}
		return ok
	})
	if n := len(walk(t, replica.root)); n > 0 {
		t.Fatalf("replica materialized %d entries from %s", n, what)
	}
}

// TestRejectsNonWriterAuthor checks the receiver-side write enforcement: manifests whose
// author is not a roster writer are dropped on apply and never materialize.
func TestRejectsNonWriterAuthor(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	replica.authorWriter = func(_ context.Context, author string) (bool, error) {
		return author != ownerID, nil
	}

	writeFile(t, owner.root, "secret.txt", []byte("written by a non-writer"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	assertRejected(t, replica, "a non-writer's manifest")
}

// TestWriterAuthoredFailsClosed checks an author-check lookup error drops the manifest
// (fail closed) rather than applying it.
func TestWriterAuthoredFailsClosed(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	replica.authorWriter = func(context.Context, string) (bool, error) {
		return false, errTestAuthLookup
	}

	writeFile(t, owner.root, "x.txt", []byte("data"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	assertRejected(t, replica, "a manifest that failed the author check")
}

// TestAcceptsWriterAuthor is the positive control: with the author admitted as a writer,
// the same folder converges normally.
func TestAcceptsWriterAuthor(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	replica.authorWriter = func(context.Context, string) (bool, error) { return true, nil }

	writeFile(t, owner.root, "ok.txt", []byte("written by a writer"))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
}
