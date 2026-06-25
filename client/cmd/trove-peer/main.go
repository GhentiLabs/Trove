// Command trove-peer runs one Trove peer; it drives the live acceptance gate.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/node"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "trove-peer:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", ".trove", "state directory for key, cert, and config db")
	trove := flag.String("trove", "", "trove://host:port?id=<fingerprint> discovery server")
	listen := flag.String("listen", "0.0.0.0:0", "local QUIC UDP bind address")
	root := flag.String("root", "", "local folder root to share (with -share)")
	share := flag.String("share", "", "shared folder id agreed with the peer")
	peer := flag.String("peer", "", "authorize this peer node id (participating in -share)")
	syncRole := flag.String("sync-role", "owner", "one-way sync role: owner (send-only) or replica (receive-only)")
	debug := flag.Bool("debug", false, "verbose debug logging (per-dial/probe/candidate detail)")
	logJSON := flag.Bool("log-json", false, "emit structured JSON logs instead of human-readable text")
	flag.Parse()

	role, err := parseSyncRole(*syncRole)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(*dir, 0o700); err != nil {
		return err
	}
	key, err := identity.LoadOrCreateKey(filepath.Join(*dir, "node.key"))
	if err != nil {
		return err
	}
	cert, err := identity.LoadOrCreateCert(filepath.Join(*dir, "node.crt"), key)
	if err != nil {
		return err
	}
	nodeID := identity.FingerprintCert(cert.Leaf)
	fmt.Println("node id:", nodeID)

	db, err := storage.Open(storage.Options{Path: filepath.Join(*dir, "config.db"), MaxOpenConns: 1})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	cfg, err := config.Open(config.Options{DB: db, NodeID: nodeID})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := pair(ctx, cfg, *root, *share, *peer); err != nil {
		return err
	}
	if *trove == "" {
		fmt.Fprintln(os.Stderr, "no -trove server given; printed node id and exiting")
		return nil
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
		StateDir: *dir, SyncRole: role,
	})
	if err != nil {
		return err
	}
	fmt.Println("running; press Ctrl-C to stop")
	return svc.Run(ctx)
}

func pair(ctx context.Context, cfg *config.Store, root, share, peer string) error {
	if share != "" {
		switch err := cfg.AddFolder(ctx, config.Folder{ID: share, Root: root, ShareID: share}); {
		case err == nil:
		case isExists(err):
			fmt.Fprintf(os.Stderr, "trove-peer: folder %q already configured; -root unchanged\n", share)
		default:
			return err
		}
	}
	if peer != "" {
		var folders []string
		if share != "" {
			folders = []string{share}
		}
		switch err := cfg.AddPeer(ctx, config.Peer{NodeID: peer, Folders: folders}); {
		case err == nil:
		case isExists(err):
			fmt.Fprintf(os.Stderr, "trove-peer: peer %s already authorized; grants unchanged\n", peer)
		default:
			return err
		}
	}
	return nil
}

func isExists(err error) bool {
	return errors.Is(err, config.ErrFolderExists) || errors.Is(err, config.ErrPeerExists)
}

func parseSyncRole(s string) (syncengine.Role, error) {
	switch s {
	case "owner":
		return syncengine.RoleOwner, nil
	case "replica":
		return syncengine.RoleReplica, nil
	default:
		return 0, fmt.Errorf("invalid -sync-role %q (want owner or replica)", s)
	}
}
