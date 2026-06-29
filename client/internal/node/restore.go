package node

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/holder"
	"github.com/GhentiLabs/Trove/client/internal/peermgr"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/storage"
)

// RestoreOptions configures RestoreFromHolder.
type RestoreOptions struct {
	Cert      tls.Certificate
	NodeID    string
	TroveURL  string
	UDPAddr   string
	HolderID  string
	FolderID  string
	MasterKey [crypto.MasterKeyLen]byte
	Root      string
	Logger    *slog.Logger
}

// RestoreFromHolder recovers an encrypted folder from a holder using only the master key
// from its recovery code. It dials the holder (discovery then holepunch), proves key
// knowledge by advertising the folder verifier, then fetches and decrypts the sealed
// catalog and chunks into Root. The holder serves read-only and never learns the key.
func RestoreFromHolder(ctx context.Context, opts RestoreOptions) error {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	udp := opts.UDPAddr
	if udp == "" {
		udp = "0.0.0.0:0"
	}
	svc, err := New(Options{Cert: opts.Cert, NodeID: opts.NodeID, TroveURL: opts.TroveURL, UDPAddr: udp, Logger: log})
	if err != nil {
		return err
	}
	defer svc.close()

	if sig, err := svc.client.Signal(ctx); err != nil {
		log.Warn("node: signal connect", "err", err)
	} else {
		svc.setSignaler(sig)
	}
	svc.gather(ctx)

	ladder := peermgr.NewLadder(peermgr.LadderConfig{
		Self: opts.NodeID, Cache: svc.cache, Dial: svc.tr.Dial, Probe: svc.tr.Probe,
		Lookup: svc.lookup, Signal: svc.signal, Candidates: svc.candidates, Logger: log,
		ForceDial: true,
	})
	conn, err := ladder.Connect(ctx, opts.HolderID)
	if err != nil {
		return fmt.Errorf("node: connect holder %s: %w", opts.HolderID, err)
	}
	defer func() { _ = conn.Close() }()

	verifier := crypto.FolderVerifier(opts.MasterKey, opts.FolderID)
	sess, err := session.Handshake(ctx, session.Config{
		Conn:      conn,
		Initiator: true,
		Local: session.Local{
			NodeID:        opts.NodeID,
			Folders:       []session.Folder{{ShareID: opts.FolderID, Encrypted: true, EncryptionVerifier: verifier}},
			ClientName:    "trove",
			ClientVersion: "m6",
		},
		Authorize: func(context.Context, string) ([]string, bool, error) { return []string{opts.FolderID}, true, nil },
		Logger:    log,
	})
	if err != nil {
		return fmt.Errorf("node: holder handshake: %w", err)
	}
	defer func() { _ = sess.Close() }()

	stage, err := os.MkdirTemp("", "trove-restore-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stage) }()
	cdb, err := storage.Open(storage.Options{Path: filepath.Join(stage, "chunk.db"), MaxOpenConns: 4})
	if err != nil {
		return err
	}
	defer func() { _ = cdb.Close() }()
	chunks, err := chunkstore.Open(chunkstore.Options{DB: cdb, BlobDir: filepath.Join(stage, "blobs"), Logger: log})
	if err != nil {
		return err
	}
	defer func() { _ = chunks.Close() }()

	fc := chunkstore.FolderContext{Encrypted: true, MasterKey: opts.MasterKey}
	get := holder.GetBlobOverConn(sess.Conn(), opts.FolderID)
	if err := holder.Restore(ctx, opts.MasterKey, chunks, fc, opts.Root, get); err != nil {
		return fmt.Errorf("node: restore folder %s: %w", opts.FolderID, err)
	}
	return nil
}
