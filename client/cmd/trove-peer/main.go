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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
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
  status     show each folder's role, root, and last-synced receipts
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
	case "status":
		return cmdStatus(rest)
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

func cmdFound(args []string) error {
	fs := flag.NewFlagSet("found", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	root := fs.String("root", "", "local folder root to share")
	encrypted := fs.Bool("encrypted", false, "encrypt the folder at rest and on untrusted holders")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *root == "" {
		return errors.New("found: -root is required")
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	ctx := context.Background()
	group, err := p.members.Found(ctx)
	if err != nil {
		return err
	}
	if err := p.cfg.AddFolder(ctx, config.Folder{ID: group, Root: *root, ShareID: group, Encrypted: *encrypted}); err != nil {
		return err
	}
	fmt.Println("group id:", group)
	if *encrypted {
		key, err := p.cfg.GenerateFolderKey(ctx, group)
		if err != nil {
			return err
		}
		fmt.Println("recovery code:", config.EncodeRecoveryCode(key))
		fmt.Println("store the recovery code safely; it is the only way to recover this folder without a member online")
	}
	fmt.Println("share this id with members; collect their `identity` output to invite them")
	return nil
}

func cmdInvite(args []string) error {
	fs := flag.NewFlagSet("invite", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	group := fs.String("group", "", "group id you own")
	nodeID := fs.String("node", "", "member node id to admit")
	keyHex := fs.String("key", "", "member public key in hex (from their `identity`)")
	writer := fs.Bool("writer", false, "admit as a writer (two-way sync) instead of a reader")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" || *nodeID == "" || *keyHex == "" {
		return errors.New("invite: -group, -node, and -key are required")
	}
	pub, err := hex.DecodeString(*keyHex)
	if err != nil {
		return fmt.Errorf("invite: bad -key: %w", err)
	}
	role, tier := membership.RoleReader, "reader"
	if *writer {
		role, tier = membership.RoleWriter, "writer"
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *group == "" || *root == "" {
		return errors.New("join: -group and -root are required")
	}
	if _, ok := membership.Founder(*group); !ok {
		return fmt.Errorf("join: %q is not a valid group id", *group)
	}
	p, err := openPeer(*dir)
	if err != nil {
		return err
	}
	defer p.close()
	ctx := context.Background()
	if err := p.members.Join(ctx, *group); err != nil {
		return err
	}
	switch err := p.cfg.AddFolder(ctx, config.Folder{ID: *group, Root: *root, ShareID: *group}); {
	case err == nil, errors.Is(err, config.ErrFolderExists):
	default:
		return err
	}
	fmt.Printf("joined %s; run `trove-peer run -trove ...` to sync\n", *group)
	return nil
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	dir := fs.String("dir", ".trove", "state directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, _, nodeID, err := loadIdentity(*dir)
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
	ctx := context.Background()
	folders, err := cfg.ListFolders(ctx)
	if err != nil {
		return err
	}
	fmt.Println("node id:", nodeID)
	for _, f := range folders {
		if f.ShareID == "" {
			continue
		}
		role := "reader"
		if founder, ok := membership.Founder(f.ShareID); ok && founder == nodeID {
			role = "owner"
		}
		fmt.Printf("\nfolder %s (%s)\n  root: %s\n", f.ShareID, role, f.Root)
		if err := printFolderReceipts(ctx, *dir, f.ID, nodeID); err != nil {
			fmt.Printf("  receipts: unavailable (%v)\n", err)
		}
	}
	return nil
}

func printFolderReceipts(ctx context.Context, dir, folderID, nodeID string) error {
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
