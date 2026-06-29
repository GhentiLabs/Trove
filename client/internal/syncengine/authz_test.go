package syncengine

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errTestAuthLookup = errors.New("author lookup failed")

// TestRejectsNonWriterAuthor checks the receiver-side write enforcement: manifests whose
// author is not a roster writer are dropped on apply and never materialize.
func TestRejectsNonWriterAuthor(t *testing.T) {
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

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(walk(t, replica.root)) > 0 {
			t.Fatal("replica materialized a manifest authored by a non-writer")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestWriterAuthoredFailsClosed checks an author-check lookup error drops the manifest
// (fail closed) rather than applying it.
func TestWriterAuthoredFailsClosed(t *testing.T) {
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

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(walk(t, replica.root)) > 0 {
			t.Fatal("replica applied a manifest despite an author-check error")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAcceptsWriterAuthor is the positive control: with the author admitted as a writer,
// the same folder converges normally.
func TestAcceptsWriterAuthor(t *testing.T) {
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
