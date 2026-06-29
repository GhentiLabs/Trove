package syncengine

import (
	"bytes"
	"context"
	"errors"
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
// sharing a key, bit-exact, with the chunks landing sealed at rest on both.
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
	assertChunkSealedAtRest(t, owner, []byte("hello world"), key)
	assertChunkSealedAtRest(t, replica, []byte("hello world"), key)
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

// assertChunkSealedAtRest proves a single-chunk file's content is stored sealed:
// a keyless read fails with ErrNoKey, while a keyed read returns the plaintext.
func assertChunkSealedAtRest(t *testing.T, p peer, content []byte, key [crypto.MasterKeyLen]byte) {
	t.Helper()
	id := hasher.Sum(content)
	ctx := context.Background()
	if _, err := p.chunks.Get(ctx, chunkstore.FolderContext{}, id); !errors.Is(err, chunkstore.ErrNoKey) {
		t.Fatalf("keyless Get err = %v, want ErrNoKey (chunk not sealed at rest)", err)
	}
	got, err := p.chunks.Get(ctx, chunkstore.FolderContext{Encrypted: true, MasterKey: key}, id)
	if err != nil {
		t.Fatalf("keyed Get: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("keyed Get returned %q, want %q", got, content)
	}
}
