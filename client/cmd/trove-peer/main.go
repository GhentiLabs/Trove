// Command trove-peer runs one Trove peer and manages its folder groups.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/control"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/node"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

const usage = `trove-peer <command> [flags]

commands:
  identity   print this node's id and public key
  found      create a folder group this node owns (-root)
  invite     admit a reader to a group you own (-group -node -key)
  join       join a group founded by someone else (-group -root)
  run        run the peer: sync and membership gossip (-trove)
  restore    recover a folder from any source (-group -code -from -root)
  status     show each folder's role, root, and last-synced receipts
  peers      show the running daemon's live peer connections
  quota      change a folder's storage cap (-group -quota)
  remove     stop syncing a folder and drop it from config (-group [-purge])

found, invite, join, status, quota, and remove talk to the running daemon when
one owns the state dir, so changes take effect without a restart; otherwise
they work on the on-disk state directly.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "trove-peer:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("no command given")
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "identity":
		return cmdIdentity(rest)
	case "found":
		return cmdFound(rest)
	case "invite":
		return cmdInvite(rest)
	case "join":
		return cmdJoin(rest)
	case "run":
		return cmdRun(rest)
	case "restore":
		return cmdRestore(rest)
	case "status":
		return cmdStatus(rest)
	case "peers":
		return cmdPeers(rest)
	case "quota":
		return cmdQuota(rest)
	case "remove":
		return cmdRemove(rest)
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// peer holds a node's identity and its open config and roster stores.
type peer struct {
	key     ed25519.PrivateKey
	cert    tls.Certificate
	nodeID  string
	cfg     *config.Store
	members *membership.Store
	close   func()
}

// daemonClient connects to the daemon owning dir's control socket. ok is false
// when no daemon is running (or the socket is stale); callers then fall back to
// the direct on-disk stores. A socket that exists but does not answer gets a
// stderr warning, since a busy daemon will not see a direct change until it
// reloads or restarts.
func daemonClient(dir string) (*control.Client, bool) {
	if _, err := os.Stat(control.SocketPath(dir)); err != nil {
		return nil, false
	}
	c := control.Dial(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := c.Identity(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "trove-peer: a control socket exists but the daemon did not answer; working on the on-disk state directly (a running daemon will not see this until it reloads)")
		return nil, false
	}
	return c, true
}

func loadIdentity(dir string) (ed25519.PrivateKey, tls.Certificate, string, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, tls.Certificate{}, "", err
	}
	key, err := identity.LoadOrCreateKey(filepath.Join(dir, "node.key"))
	if err != nil {
		return nil, tls.Certificate{}, "", err
	}
	cert, err := identity.LoadOrCreateCert(filepath.Join(dir, "node.crt"), key)
	if err != nil {
		return nil, tls.Certificate{}, "", err
	}
	return key, cert, identity.FingerprintCert(cert.Leaf), nil
}

func openPeer(dir string) (*peer, error) {
	key, cert, nodeID, err := loadIdentity(dir)
	if err != nil {
		return nil, err
	}
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "config.db"), MaxOpenConns: 1})
	if err != nil {
		return nil, err
	}
	cfg, err := config.Open(config.Options{DB: cdb, NodeID: nodeID})
	if err != nil {
		_ = cdb.Close()
		return nil, err
	}
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(dir, "membership.db"), MaxOpenConns: 4})
	if err != nil {
		_ = cdb.Close()
		return nil, err
	}
	members, err := membership.Open(membership.Options{DB: mdb, NodeID: nodeID, Key: key})
	if err != nil {
		_ = mdb.Close()
		_ = cdb.Close()
		return nil, err
	}
	return &peer{
		key: key, cert: cert, nodeID: nodeID, cfg: cfg, members: members,
		close: func() { _ = mdb.Close(); _ = cdb.Close() },
	}, nil
}

func cmdIdentity(args []string) error {
	fs := flag.NewFlagSet("identity", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory for key, cert, and databases")
	if err := fs.Parse(args); err != nil {
		return err
	}
	key, _, nodeID, err := loadIdentity(*dir)
	if err != nil {
		return err
	}
	fmt.Println("node id:", nodeID)
	fmt.Println("public key:", hex.EncodeToString(key.Public().(ed25519.PublicKey)))
	return nil
}

// parseSize parses a byte count with an optional KB/MB/GB/TB suffix (1000-based); a bare
// number is bytes and "" or "0" is unlimited.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	unit := int64(1)
	if len(s) >= 2 {
		switch strings.ToLower(s[len(s)-2:]) {
		case "kb":
			unit = 1e3
		case "mb":
			unit = 1e6
		case "gb":
			unit = 1e9
		case "tb":
			unit = 1e12
		}
		if unit != 1 {
			s = strings.TrimSpace(s[:len(s)-2])
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q: want a whole number optionally suffixed KB/MB/GB/TB", s)
	}
	if n > math.MaxInt64/unit {
		return 0, fmt.Errorf("size %q overflows", s)
	}
	return n * unit, nil
}

func formatSize(n int64) string {
	switch {
	case n >= 1e12:
		return fmt.Sprintf("%.1fTB", float64(n)/1e12)
	case n >= 1e9:
		return fmt.Sprintf("%.1fGB", float64(n)/1e9)
	case n >= 1e6:
		return fmt.Sprintf("%.1fMB", float64(n)/1e6)
	case n >= 1e3:
		return fmt.Sprintf("%.1fKB", float64(n)/1e3)
	default:
		return fmt.Sprintf("%dB", n)
	}
}

func cmdFound(args []string) error {
	fs := flag.NewFlagSet("found", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	root := fs.String("root", "", "local folder root to share")
	encrypted := fs.Bool("encrypted", false, "encrypt the folder at rest and on untrusted holders")
	syncOnly := fs.Bool("sync", false, "sync-only: keep no version history, stay at ~1x disk (default keeps history)")
	quota := fs.String("quota", "", "max storage this node lends the folder, e.g. 10GB or 500MB (default unlimited)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *root == "" {
		return errors.New("found: -root is required")
	}
	quotaBytes, err := parseSize(*quota)
	if err != nil {
		return fmt.Errorf("found: -quota: %w", err)
	}
	ctx := context.Background()
	if c, ok := daemonClient(*dir); ok {
		// The daemon resolves paths in its own working directory, so pin the
		// root before it crosses the socket.
		abs, err := filepath.Abs(*root)
		if err != nil {
			return err
		}
		resp, err := c.Found(ctx, control.FoundRequest{Root: abs, Encrypted: *encrypted, KeepHistory: !*syncOnly, QuotaBytes: quotaBytes})
		if err != nil {
			return err
		}
		printFounded(resp.GroupID, resp.RecoveryCode)
		return nil
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	group, err := p.members.Found(ctx)
	if err != nil {
		return err
	}
	if err := p.cfg.AddFolder(ctx, config.Folder{ID: group, Root: *root, ShareID: group, Encrypted: *encrypted, KeepHistory: !*syncOnly, QuotaBytes: quotaBytes}); err != nil {
		return err
	}
	var secret [config.MasterKeyLen]byte
	if *encrypted {
		secret, err = p.cfg.GenerateFolderKey(ctx, group)
	} else {
		secret, err = p.cfg.GenerateRecoverySecret(ctx, group)
	}
	if err != nil {
		return err
	}
	printFounded(group, config.EncodeRecoveryCode(secret))
	return nil
}

func printFounded(group, recoveryCode string) {
	fmt.Println("group id:", group)
	fmt.Println("recovery code:", recoveryCode)
	fmt.Println("store the recovery code safely; it is the only way to recover this folder without a member online")
	fmt.Println("share this id with members; collect their `identity` output to invite them")
}

func cmdInvite(args []string) error {
	fs := flag.NewFlagSet("invite", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	group := fs.String("group", "", "group id you own")
	nodeID := fs.String("node", "", "member node id to admit")
	keyHex := fs.String("key", "", "member public key in hex (from their `identity`)")
	writer := fs.Bool("writer", false, "admit as a writer (two-way sync) instead of a reader")
	holderFlag := fs.Bool("holder", false, "admit as an untrusted holder (stores ciphertext only, no key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" || *nodeID == "" || *keyHex == "" {
		return errors.New("invite: -group, -node, and -key are required")
	}
	if *writer && *holderFlag {
		return errors.New("invite: -writer and -holder are mutually exclusive")
	}
	pub, err := hex.DecodeString(*keyHex)
	if err != nil {
		return fmt.Errorf("invite: bad -key: %w", err)
	}
	role, tier := membership.RoleReader, "reader"
	switch {
	case *writer:
		role, tier = membership.RoleWriter, "writer"
	case *holderFlag:
		role, tier = membership.RoleHolder, "holder"
	}
	if c, ok := daemonClient(*dir); ok {
		if err := c.Invite(context.Background(), *group, control.InviteRequest{NodeID: *nodeID, PublicKey: *keyHex, Role: tier}); err != nil {
			return err
		}
		fmt.Printf("admitted %s as a %s of %s\n", *nodeID, tier, *group)
		return nil
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	if _, err := p.members.Add(context.Background(), *group, *nodeID, pub, role); err != nil {
		return err
	}
	fmt.Printf("admitted %s as a %s of %s\n", *nodeID, tier, *group)
	return nil
}

func cmdJoin(args []string) error {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	group := fs.String("group", "", "group id to join (from the owner)")
	root := fs.String("root", "", "local folder root to receive into")
	encrypted := fs.Bool("encrypted", false, "the folder is encrypted; await the key over the session")
	holderFlag := fs.Bool("holder", false, "join as a holder: store ciphertext only, no root or key")
	syncOnly := fs.Bool("sync", false, "sync-only: this node keeps no version history for the folder, staying at ~1x disk")
	quota := fs.String("quota", "", "max storage this node lends the folder, e.g. 10GB or 500MB (default unlimited)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("join: -group is required")
	}
	if !*holderFlag && *root == "" {
		return errors.New("join: -root is required")
	}
	quotaBytes, err := parseSize(*quota)
	if err != nil {
		return fmt.Errorf("join: -quota: %w", err)
	}
	if _, ok := membership.Founder(*group); !ok {
		return fmt.Errorf("join: %q is not a valid group id", *group)
	}
	ctx := context.Background()
	if c, ok := daemonClient(*dir); ok {
		rootArg := *root
		if rootArg != "" {
			abs, err := filepath.Abs(rootArg)
			if err != nil {
				return err
			}
			rootArg = abs
		}
		req := control.JoinRequest{GroupID: *group, Root: rootArg, Encrypted: *encrypted, Holder: *holderFlag, KeepHistory: !*syncOnly, QuotaBytes: quotaBytes}
		if err := c.Join(ctx, req); err != nil {
			return err
		}
		printJoined(*group, *holderFlag, true)
		return nil
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	if err := p.members.Join(ctx, *group); err != nil {
		return err
	}
	folder := config.Folder{ID: *group, Root: *root, ShareID: *group, Encrypted: *encrypted || *holderFlag, Holder: *holderFlag, KeepHistory: !*syncOnly, QuotaBytes: quotaBytes}
	switch err := p.cfg.AddFolder(ctx, folder); {
	case err == nil, errors.Is(err, config.ErrFolderExists):
	default:
		return err
	}
	printJoined(*group, *holderFlag, false)
	return nil
}

func printJoined(group string, holder, live bool) {
	if holder {
		fmt.Println("joined as a holder: this node stores ciphertext only and never receives the key")
	}
	if live {
		fmt.Printf("joined %s; the running daemon is syncing it\n", group)
		return
	}
	fmt.Printf("joined %s; run `trove-peer run -trove ...` to sync\n", group)
}

func cmdRestore(args []string) error {
	fs := flag.NewFlagSet("restore", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory (a fresh one mints a new identity)")
	trove := fs.String("trove", "", "trove://host:port?id=<fingerprint> discovery server")
	group := fs.String("group", "", "group id of the folder to restore")
	code := fs.String("code", "", "recovery code printed when the folder was founded")
	var sources []string
	fs.Func("from", "node id to recover from (any member or holder; repeatable)", func(s string) error {
		sources = append(sources, s)
		return nil
	})
	root := fs.String("root", "", "local directory to restore the folder into")
	listen := fs.String("listen", "0.0.0.0:0", "local QUIC UDP bind address")
	debug := fs.Bool("debug", false, "verbose debug logging")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *trove == "" || *group == "" || *code == "" || len(sources) == 0 || *root == "" {
		return errors.New("restore: -trove, -group, -code, -from, and -root are required")
	}
	if _, ok := membership.Founder(*group); !ok {
		return fmt.Errorf("restore: %q is not a valid group id", *group)
	}
	secret, err := config.DecodeRecoveryCode(*code)
	if err != nil {
		return err
	}
	_, cert, nodeID, err := loadIdentity(*dir)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if err := os.MkdirAll(*root, 0o700); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	fmt.Printf("recovering %s from %v...\n", *group, sources)
	if err := node.Recover(ctx, node.RecoverOptions{
		Cert: cert, NodeID: nodeID, TroveURL: *trove, UDPAddr: *listen,
		Sources: sources, ShareID: *group, Secret: secret, Root: *root, Logger: log,
	}); err != nil {
		return err
	}
	fmt.Printf("recovered %s into %s\n", *group, *root)
	return nil
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if c, ok := daemonClient(*dir); ok {
		return printLiveStatus(context.Background(), c)
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	ctx := context.Background()
	folders, err := p.cfg.ListFolders(ctx)
	if err != nil {
		return err
	}
	fmt.Println("node id:", p.nodeID)
	for _, f := range folders {
		if f.ShareID == "" {
			continue
		}
		fmt.Printf("\nfolder %s (%s)\n  root: %s\n", f.ShareID, folderRoleName(ctx, p, f), f.Root)
		if err := printFolderReceipts(ctx, *dir, f.ID, p.nodeID, f.QuotaBytes); err != nil {
			fmt.Printf("  receipts: unavailable (%v)\n", err)
		}
	}
	return nil
}

// folderRoleName mirrors the daemon's role derivation for the offline status path.
func folderRoleName(ctx context.Context, p *peer, f config.Folder) string {
	if f.Holder {
		return "holder"
	}
	if founder, ok := membership.Founder(f.ShareID); ok && founder == p.nodeID {
		return "owner"
	}
	if role, ok, err := p.members.RoleOf(ctx, f.ShareID, p.nodeID); err == nil && ok && role == membership.RoleWriter {
		return "writer"
	}
	return "reader"
}

// printLiveStatus renders the running daemon's view in the same shape as the
// direct on-disk path below, which the e2e harness parses.
func printLiveStatus(ctx context.Context, c *control.Client) error {
	st, err := c.Status(ctx)
	if err != nil {
		return err
	}
	fmt.Println("node id:", st.NodeID)
	for _, f := range st.Folders {
		fmt.Printf("\nfolder %s (%s)\n  root: %s\n", f.ID, f.Role, f.Root)
		if !f.Synced {
			fmt.Println("  not yet synced")
			continue
		}
		limit := "unlimited"
		if f.QuotaBytes > 0 {
			limit = formatSize(f.QuotaBytes)
		}
		fmt.Printf("  storage: %s / %s\n", formatSize(f.UsedBytes), limit)
		if len(f.Receipts) == 0 {
			fmt.Println("  not yet synced")
			continue
		}
		for _, r := range f.Receipts {
			fmt.Printf("  peer %s  seq=%d  last synced %s\n",
				r.PeerID, r.HighWater, r.SyncedAt.Format(time.RFC3339))
		}
	}
	return nil
}

func printFolderReceipts(ctx context.Context, dir, folderID, nodeID string, quota int64) error {
	path := filepath.Join(dir, "folders", folderID, "model.db")
	if _, err := os.Stat(path); err != nil {
		fmt.Println("  not yet synced")
		return nil
	}
	mdb, err := storage.Open(storage.Options{Path: path, MaxOpenConns: 1})
	if err != nil {
		return err
	}
	defer func() { _ = mdb.Close() }()
	ms, err := model.Open(model.Options{DB: mdb, NodeID: nodeID})
	if err != nil {
		return err
	}
	used, err := ms.ReachableLogicalBytes(ctx)
	if err != nil {
		return err
	}
	limit := "unlimited"
	if quota > 0 {
		limit = formatSize(quota)
	}
	fmt.Printf("  storage: %s / %s\n", formatSize(used), limit)
	receipts, err := ms.Receipts(ctx, model.LocalSync)
	if err != nil {
		return err
	}
	if len(receipts) == 0 {
		fmt.Println("  not yet synced")
		return nil
	}
	for _, r := range receipts {
		fmt.Printf("  peer %s  seq=%d  last synced %s\n",
			r.PeerID, r.HighWater, r.SyncedAt.Format(time.RFC3339))
	}
	return nil
}

func cmdPeers(args []string) error {
	fs := flag.NewFlagSet("peers", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, ok := daemonClient(*dir)
	if !ok {
		return errors.New("peers: no daemon is running on this state dir")
	}
	peers, err := c.Peers(context.Background())
	if err != nil {
		return err
	}
	if len(peers.Active) == 0 {
		fmt.Println("no connected peers")
		return nil
	}
	for _, p := range peers.Active {
		fmt.Printf("peer %s  folders: %s\n", p.NodeID, strings.Join(p.Folders, ", "))
	}
	return nil
}

func cmdQuota(args []string) error {
	fs := flag.NewFlagSet("quota", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	group := fs.String("group", "", "group id of the folder")
	quota := fs.String("quota", "", "new cap, e.g. 10GB or 500MB; 0 or empty is unlimited")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("quota: -group is required")
	}
	quotaBytes, err := parseSize(*quota)
	if err != nil {
		return fmt.Errorf("quota: -quota: %w", err)
	}
	ctx := context.Background()
	if c, ok := daemonClient(*dir); ok {
		f, err := c.SetQuota(ctx, *group, quotaBytes)
		if err != nil {
			return err
		}
		printQuotaSet(*group, quotaBytes)
		if f.QuotaWarning != "" {
			fmt.Println("warning:", f.QuotaWarning)
		}
		return nil
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	if err := p.cfg.SetFolderQuota(ctx, *group, quotaBytes); err != nil {
		return err
	}
	printQuotaSet(*group, quotaBytes)
	return nil
}

func printQuotaSet(group string, quota int64) {
	limit := "unlimited"
	if quota > 0 {
		limit = formatSize(quota)
	}
	fmt.Printf("quota for %s set to %s\n", group, limit)
}

func cmdRemove(args []string) error {
	fs := flag.NewFlagSet("remove", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	group := fs.String("group", "", "group id of the folder to remove")
	purge := fs.Bool("purge", false, "also delete the folder's local state (history and chunk stores); working files are never touched")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" {
		return errors.New("remove: -group is required")
	}
	ctx := context.Background()
	if c, ok := daemonClient(*dir); ok {
		if err := c.Remove(ctx, *group, *purge); err != nil {
			return err
		}
		printRemoved(*group, *purge)
		return nil
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	if err := p.cfg.RemoveFolder(ctx, *group); err != nil {
		return err
	}
	if *purge {
		if err := os.RemoveAll(filepath.Join(*dir, "folders", *group)); err != nil {
			return fmt.Errorf("remove: purge folder state: %w", err)
		}
	}
	printRemoved(*group, *purge)
	return nil
}

func printRemoved(group string, purge bool) {
	fmt.Printf("removed %s; working files were left in place\n", group)
	if !purge {
		fmt.Println("its local history remains under the state dir; re-run with -purge to delete it")
	}
	fmt.Println("this node remains on the group's roster; other members still count it a member")
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	trove := fs.String("trove", "", "trove://host:port?id=<fingerprint> discovery server")
	listen := fs.String("listen", "0.0.0.0:0", "local QUIC UDP bind address")
	debug := fs.Bool("debug", false, "verbose debug logging (per-dial/probe/candidate detail)")
	logJSON := fs.Bool("log-json", false, "emit structured JSON logs instead of human-readable text")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *trove == "" {
		return errors.New("run: -trove is required")
	}
	_, cert, nodeID, err := loadIdentity(*dir)
	if err != nil {
		return err
	}
	db, err := storage.Open(storage.Options{Path: filepath.Join(*dir, "config.db"), MaxOpenConns: 1})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	cfg, err := config.Open(config.Options{DB: db, NodeID: nodeID})
	if err != nil {
		return err
	}

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if *logJSON {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	log := slog.New(handler)

	svc, err := node.New(node.Options{
		Cert: cert, NodeID: nodeID, Config: cfg, TroveURL: *trove, UDPAddr: *listen, Logger: log,
		StateDir: *dir,
	})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	fmt.Println("node id:", nodeID)
	fmt.Println("running; press Ctrl-C to stop")
	return svc.Run(ctx)
}
