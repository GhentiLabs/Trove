package node

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/control"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

// deadTroveURL is a valid trove:// URL nothing listens on; discovery loops warn
// and retry harmlessly.
func deadTroveURL(t *testing.T) string {
	t.Helper()
	pub, _, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	id, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	return "trove://127.0.0.1:9?id=" + id
}

// shortTempDir keeps the state dir short enough for a unix socket path.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "trovectl")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

type daemonFixture struct {
	dir    string
	nodeID string
	cert   tls.Certificate
	client *control.Client
	done   chan error
}

func newDaemon(t *testing.T, dir string) (*Service, string, tls.Certificate) {
	t.Helper()
	pub, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	nodeID, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	cert, err := identity.NewCertificate(key)
	if err != nil {
		t.Fatalf("NewCertificate: %v", err)
	}
	return newDaemonAs(t, dir, cert, nodeID), nodeID, cert
}

// newDaemonAs builds a Service for dir under a fixed identity, as a second
// process sharing the state dir would.
func newDaemonAs(t *testing.T, dir string, cert tls.Certificate, nodeID string) *Service {
	t.Helper()
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "config.db"), MaxOpenConns: 1})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = cdb.Close() })
	cfg, err := config.Open(config.Options{DB: cdb, NodeID: nodeID})
	if err != nil {
		t.Fatalf("config.Open: %v", err)
	}
	svc, err := New(Options{Cert: cert, NodeID: nodeID, Config: cfg, TroveURL: deadTroveURL(t), UDPAddr: "127.0.0.1:0", StateDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

// startDaemon runs a full Service on a temp state dir and waits for its control
// socket to answer.
func startDaemon(t *testing.T) daemonFixture {
	t.Helper()
	dir := shortTempDir(t)
	svc, nodeID, cert := newDaemon(t, dir)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("daemon did not stop")
		}
	})

	c := control.Dial(dir)
	waitFor(t, 15*time.Second, func() bool {
		_, err := c.Identity(context.Background())
		return err == nil
	}, "control socket never answered")
	return daemonFixture{dir: dir, nodeID: nodeID, cert: cert, client: c, done: done}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestControlIdentityAndEmptyStatus(t *testing.T) {
	f := startDaemon(t)
	ctx := context.Background()

	id, err := f.client.Identity(ctx)
	if err != nil || id.NodeID != f.nodeID || id.PublicKey == "" {
		t.Fatalf("Identity = %+v, %v", id, err)
	}
	st, err := f.client.Status(ctx)
	if err != nil || st.NodeID != f.nodeID || len(st.Folders) != 0 {
		t.Fatalf("Status = %+v, %v", st, err)
	}
	peers, err := f.client.Peers(ctx)
	if err != nil || len(peers.Active) != 0 {
		t.Fatalf("Peers = %+v, %v", peers, err)
	}
}

func TestControlFoundTakesEffectLive(t *testing.T) {
	f := startDaemon(t)
	ctx := context.Background()

	root := shortTempDir(t)
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("hello control"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	resp, err := f.client.Found(ctx, control.FoundRequest{Root: root, KeepHistory: true})
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	if resp.GroupID == "" || resp.RecoveryCode == "" {
		t.Fatalf("Found = %+v", resp)
	}

	// The rebuilt cycle attaches the folder and its scanner ingests the existing
	// file, all without a restart.
	waitFor(t, 30*time.Second, func() bool {
		st, err := f.client.Status(ctx)
		if err != nil || len(st.Folders) != 1 {
			return false
		}
		fo := st.Folders[0]
		return fo.ID == resp.GroupID && fo.Role == "owner" && fo.Synced && fo.UsedBytes > 0
	}, "found folder never went live with ingested data")
}

func TestControlRemoveFolderPurges(t *testing.T) {
	f := startDaemon(t)
	ctx := context.Background()

	root := shortTempDir(t)
	resp, err := f.client.Found(ctx, control.FoundRequest{Root: root, KeepHistory: true})
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	stateDir := filepath.Join(f.dir, "folders", resp.GroupID)
	waitFor(t, 30*time.Second, func() bool {
		_, err := os.Stat(stateDir)
		return err == nil
	}, "folder state dir never created")

	if err := f.client.Remove(ctx, resp.GroupID, true); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := os.Stat(stateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("purged state dir still present: %v", err)
	}
	st, err := f.client.Status(ctx)
	if err != nil || len(st.Folders) != 0 {
		t.Fatalf("Status after remove = %+v, %v", st, err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("working files were deleted by remove: %v", err)
	}
}

func TestControlQuotaPatch(t *testing.T) {
	f := startDaemon(t)
	ctx := context.Background()

	root := shortTempDir(t)
	resp, err := f.client.Found(ctx, control.FoundRequest{Root: root, KeepHistory: true, QuotaBytes: 1000})
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		st, err := f.client.Status(ctx)
		return err == nil && len(st.Folders) == 1 && st.Folders[0].Synced
	}, "folder never attached")

	fo, err := f.client.SetQuota(ctx, resp.GroupID, 5<<20)
	if err != nil || fo.QuotaBytes != 5<<20 {
		t.Fatalf("SetQuota = %+v, %v", fo, err)
	}
	st, err := f.client.Status(ctx)
	if err != nil || st.Folders[0].QuotaBytes != 5<<20 {
		t.Fatalf("Status quota = %+v, %v", st, err)
	}
	if _, err := f.client.SetQuota(ctx, "not-a-folder", 1); err == nil {
		t.Fatal("SetQuota(unknown) succeeded")
	}
}

func TestControlJoinAndInvite(t *testing.T) {
	f := startDaemon(t)
	ctx := context.Background()

	pub, _, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	founderID, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	group := founderID + ".0123456789abcdef"
	root := shortTempDir(t)
	if err := f.client.Join(ctx, control.JoinRequest{GroupID: group, Root: root, KeepHistory: true}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool {
		st, err := f.client.Status(ctx)
		if err != nil || len(st.Folders) != 1 {
			return false
		}
		return st.Folders[0].ID == group && st.Folders[0].Role == "reader" && st.Folders[0].Synced
	}, "joined folder never attached as a reader")
	if err := f.client.Join(ctx, control.JoinRequest{GroupID: group, Root: root}); err == nil {
		t.Fatal("re-joining the same group succeeded")
	}

	resp, err := f.client.Found(ctx, control.FoundRequest{Root: shortTempDir(t), KeepHistory: true})
	if err != nil {
		t.Fatalf("Found: %v", err)
	}
	ipub, _, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey invitee: %v", err)
	}
	inviteeID, err := identity.FingerprintKey(ipub)
	if err != nil {
		t.Fatalf("FingerprintKey invitee: %v", err)
	}
	inv := control.InviteRequest{NodeID: inviteeID, PublicKey: hex.EncodeToString(ipub), Role: "writer"}
	if err := f.client.Invite(ctx, resp.GroupID, inv); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	inv.Role = "boss"
	if err := f.client.Invite(ctx, resp.GroupID, inv); err == nil {
		t.Fatal("invite with an unknown role succeeded")
	}
}

// TestControlConcurrentMutationsKeepInvariant races a sync-folder found against a
// holder join: at most one may win, or a later rebuild would fail the holder/sync
// mix check and kill the daemon.
func TestControlConcurrentMutationsKeepInvariant(t *testing.T) {
	f := startDaemon(t)
	ctx := context.Background()

	pub, _, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	founderID, err := identity.FingerprintKey(pub)
	if err != nil {
		t.Fatalf("FingerprintKey: %v", err)
	}
	group := founderID + ".0123456789abcdef"

	root := shortTempDir(t)
	var foundErr, joinErr error
	var wg sync.WaitGroup
	wg.Go(func() { _, foundErr = f.client.Found(ctx, control.FoundRequest{Root: root, KeepHistory: true}) })
	wg.Go(func() { joinErr = f.client.Join(ctx, control.JoinRequest{GroupID: group, Holder: true}) })
	wg.Wait()

	if foundErr == nil && joinErr == nil {
		t.Fatal("a sync folder and a holder folder were both admitted")
	}
	st, err := f.client.Status(ctx)
	if err != nil {
		t.Fatalf("daemon unresponsive after concurrent mutations: %v", err)
	}
	for _, fo := range st.Folders {
		if fo.Holder != st.Folders[0].Holder {
			t.Fatalf("mixed holder/sync folders in config: %+v", st.Folders)
		}
	}
}

func TestControlRefusesSecondDaemon(t *testing.T) {
	f := startDaemon(t)

	second := newDaemonAs(t, f.dir, f.cert, f.nodeID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := second.Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second daemon Run = %v, want a socket-in-use error", err)
	}
}

func TestControlRemovesStaleSocket(t *testing.T) {
	dir := shortTempDir(t)
	// A leftover file at the socket path (not a live listener) must not block startup.
	if err := os.WriteFile(control.SocketPath(dir), nil, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	svc, _, _ := newDaemon(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	c := control.Dial(dir)
	waitFor(t, 15*time.Second, func() bool {
		_, err := c.Identity(context.Background())
		return err == nil
	}, "daemon never recovered from a stale socket file")
}
