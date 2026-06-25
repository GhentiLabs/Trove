package syncengine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/netio"
	"github.com/GhentiLabs/Trove/client/internal/scanner"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/watcher"
)

const folderID = "demo"

func grant(string) ([]string, bool, error) { return []string{folderID}, true, nil }

type peer struct {
	id     string
	root   string
	model  *model.Store
	chunks *chunkstore.Store
	fc     chunkstore.FolderContext
}

func newPeer(t *testing.T, id string) peer {
	t.Helper()
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "model.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("model db: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	ms, err := model.Open(model.Options{DB: mdb, NodeID: id})
	if err != nil {
		t.Fatalf("model open: %v", err)
	}
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "chunk.db"), MaxOpenConns: 4})
	if err != nil {
		t.Fatalf("chunk db: %v", err)
	}
	t.Cleanup(func() { _ = cdb.Close() })
	cs, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(dir, "blobs")})
	if err != nil {
		t.Fatalf("chunkstore open: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return peer{id: id, root: root, model: ms, chunks: cs}
}

// scan ingests the peer's on-disk tree into its model and chunk store, as the owner.
func (p peer) scan(t *testing.T) {
	t.Helper()
	sc, err := scanner.New(scanner.Options{
		Root: p.root, FolderCtx: p.fc, Chunks: p.chunks, Model: p.model, Watcher: watcher.NewFake(),
	})
	if err != nil {
		t.Fatalf("scanner.New: %v", err)
	}
	if err := sc.Rescan(context.Background()); err != nil {
		t.Fatalf("Rescan: %v", err)
	}
	if _, err := p.model.Cut(context.Background()); err != nil {
		t.Fatalf("Cut: %v", err)
	}
}

func (p peer) currentRoot(t *testing.T) string {
	t.Helper()
	r, err := p.model.CurrentRoot(context.Background())
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}
	return r.String()
}

// startSync wires owner and replica over MemNet, installs engines, and drives them
// until ctx is cancelled. It returns both engines.
func startSync(t *testing.T, ctx context.Context, owner, replica peer, wrap ...func(netio.Conn) netio.Conn) (ownerEng, replicaEng *Engine) {
	t.Helper()
	mn := netio.NewMemNet()
	ot := mn.Transport("owner", owner.id)
	rt := mn.Transport("replica", replica.id)

	type res struct {
		s   *session.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		conn, err := ot.Accept(ctx)
		if err != nil {
			ch <- res{nil, err}
			return
		}
		s, err := session.Handshake(ctx, session.Config{
			Conn: conn, Initiator: false, Authorize: grant,
			Local: session.Local{NodeID: owner.id, Folders: []session.Folder{{ShareID: folderID}}},
		})
		ch <- res{s, err}
	}()
	rconn, err := rt.Dial(ctx, "owner", owner.id)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if len(wrap) > 0 && wrap[0] != nil {
		rconn = wrap[0](rconn)
	}
	rs, err := session.Handshake(ctx, session.Config{
		Conn: rconn, Initiator: true, Authorize: grant,
		Local: session.Local{NodeID: replica.id, Folders: []session.Folder{{ShareID: folderID}}},
	})
	if err != nil {
		t.Fatalf("replica handshake: %v", err)
	}
	or := <-ch
	if or.err != nil {
		t.Fatalf("owner handshake: %v", or.err)
	}
	return wireEngines(t, ctx, or.s, rs, owner, replica)
}

// wireEngines builds an owner and replica engine on two Active sessions, installs
// their control handlers, and drives them until ctx ends.
func wireEngines(t *testing.T, ctx context.Context, ownerSess, replicaSess *session.Session, owner, replica peer) (ownerEng, replicaEng *Engine) {
	t.Helper()
	ownerEng, err := New(Options{Session: ownerSess, Folders: []FolderConfig{{
		FolderID: folderID, Role: RoleOwner, Root: owner.root, FolderCtx: owner.fc, Model: owner.model, Chunks: owner.chunks,
	}}})
	if err != nil {
		t.Fatalf("owner engine: %v", err)
	}
	replicaEng, err = New(Options{Session: replicaSess, Folders: []FolderConfig{{
		FolderID: folderID, Role: RoleReplica, Root: replica.root, FolderCtx: replica.fc, Model: replica.model, Chunks: replica.chunks,
	}}})
	if err != nil {
		t.Fatalf("replica engine: %v", err)
	}
	ownerSess.SetControlHandler(ownerEng.Handle)
	replicaSess.SetControlHandler(replicaEng.Handle)
	go func() { _ = ownerSess.Run(ctx) }()
	go func() { _ = replicaSess.Run(ctx) }()
	go func() { _ = ownerEng.Drive(ctx) }()
	go func() { _ = replicaEng.Drive(ctx) }()
	return ownerEng, replicaEng
}

func waitConverged(t *testing.T, owner, replica peer) {
	t.Helper()
	want := owner.currentRoot(t)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if replica.currentRoot(t) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not converge: replica root %s, want %s", replica.currentRoot(t), want)
}

// assertTreesEqual checks that the replica tree is bit-exact to the owner tree:
// same set of paths, identical bytes, identical permission bits, identical symlink
// targets. tmpDirName is ignored.
func assertTreesEqual(t *testing.T, ownerRoot, replicaRoot string) {
	t.Helper()
	ownerEntries := walk(t, ownerRoot)
	replicaEntries := walk(t, replicaRoot)
	if len(ownerEntries) != len(replicaEntries) {
		t.Fatalf("entry count: owner %d, replica %d\nowner=%v\nreplica=%v",
			len(ownerEntries), len(replicaEntries), keys(ownerEntries), keys(replicaEntries))
	}
	for rel, oi := range ownerEntries {
		ri, ok := replicaEntries[rel]
		if !ok {
			t.Fatalf("replica missing %q", rel)
		}
		if oi.mode != ri.mode {
			t.Fatalf("%q mode: owner %v, replica %v", rel, oi.mode, ri.mode)
		}
		switch {
		case oi.link != "":
			if oi.link != ri.link {
				t.Fatalf("%q symlink: owner %q, replica %q", rel, oi.link, ri.link)
			}
		case !oi.dir:
			if !bytes.Equal(oi.data, ri.data) {
				t.Fatalf("%q content differs (owner %d bytes, replica %d bytes)", rel, len(oi.data), len(ri.data))
			}
		}
	}
}

type entry struct {
	dir  bool
	mode os.FileMode
	data []byte
	link string
}

func walk(t *testing.T, root string) map[string]entry {
	t.Helper()
	out := make(map[string]entry)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if rel == "." {
			return nil
		}
		if strings.HasPrefix(rel, tmpDirName) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		fi, err := os.Lstat(path)
		if err != nil {
			return err
		}
		e := entry{dir: d.IsDir(), mode: fi.Mode().Perm()}
		switch {
		case fi.Mode()&os.ModeSymlink != 0:
			if e.link, err = os.Readlink(path); err != nil {
				return err
			}
		case !d.IsDir():
			if e.data, err = os.ReadFile(path); err != nil {
				return err
			}
		}
		out[rel] = e
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return out
}

func keys(m map[string]entry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func writeFile(t *testing.T, root, rel string, data []byte) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}
