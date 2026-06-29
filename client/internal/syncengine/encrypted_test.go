package syncengine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

func testKey(b byte) [crypto.MasterKeyLen]byte {
	var k [crypto.MasterKeyLen]byte
	for i := range k {
		k[i] = b ^ byte(i)
	}
	return k
}

func encrypt(p *peer, key [crypto.MasterKeyLen]byte) {
	p.fc = chunkstore.FolderContext{Encrypted: true, MasterKey: key}
}

// TestEncryptedFolderConvergesBitExact syncs an encrypted folder between two peers
// sharing a key, bit-exact. Under M7 the owner's current data is a plaintext clone
// of the working file (not sealed at rest); transit and history sealing are
// unaffected.
func TestEncryptedFolderConvergesBitExact(t *testing.T) {
	t.Parallel()
	owner := newPeer(t, ownerID)
	replica := newPeer(t, replicaID)
	key := testKey(0xA5)
	encrypt(&owner, key)
	encrypt(&replica, key)

	writeFile(t, owner.root, "a.txt", []byte("hello world"))
	writeFile(t, owner.root, "dir/b.txt", []byte("nested file contents"))
	writeFile(t, owner.root, "big.bin", pseudoRandom(5<<20, 7))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startSync(t, ctx, owner, replica)

	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
	assertCurrentChunkPlaintext(t, owner, []byte("hello world"))
	assertCurrentChunkPlaintext(t, replica, []byte("hello world"))
}

// TestEncryptedSnapshotRootMatchesPlaintext checks that encrypting a folder changes
// no history hash: the same tree scanned encrypted and unencrypted yields a
// byte-identical snapshot root.
func TestEncryptedSnapshotRootMatchesPlaintext(t *testing.T) {
	t.Parallel()
	plain := newPeer(t, ownerID)
	sealed := newPeer(t, ownerID)
	encrypt(&sealed, testKey(0x3C))

	for _, p := range []peer{plain, sealed} {
		writeFile(t, p.root, "a.txt", []byte("hello world"))
		writeFile(t, p.root, "dir/b.txt", []byte("nested file contents"))
		writeFile(t, p.root, "big.bin", pseudoRandom(3<<20, 11))
		if err := os.Symlink("a.txt", filepath.Join(p.root, "link")); err != nil {
			t.Fatalf("symlink: %v", err)
		}
		p.scan(t)
	}

	if got, want := sealed.currentRoot(t), plain.currentRoot(t); got != want {
		t.Fatalf("encrypted root %s != plaintext root %s", got, want)
	}
}

// assertCurrentChunkPlaintext proves M7 stores current data as a plaintext clone
// even for an encrypted folder: the chunk reads back without the folder key.
func assertCurrentChunkPlaintext(t *testing.T, p peer, content []byte) {
	t.Helper()
	got, err := p.chunks.Get(context.Background(), chunkstore.FolderContext{}, hasher.Sum(content))
	if err != nil {
		t.Fatalf("keyless Get of current clone chunk: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("keyless Get returned %q, want %q", got, content)
	}
}
