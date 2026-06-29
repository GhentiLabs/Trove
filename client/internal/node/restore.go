package node

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/holder"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/peermgr"
	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/snapshot"
	"github.com/GhentiLabs/Trove/client/internal/storage"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
	"github.com/GhentiLabs/Trove/client/internal/wire"
)

// RecoverOptions configures Recover.
type RecoverOptions struct {
	Cert     tls.Certificate
	NodeID   string
	TroveURL string
	UDPAddr  string
	Sources  []string // node ids to recover from: any member or holder
	ShareID  string
	Secret   [crypto.MasterKeyLen]byte // decoded recovery code
	Root     string
	Logger   *slog.Logger
}

// Recover rebuilds a folder into Root by pulling it from any of the named sources (a member via
// the sync engine, a holder via the blob protocol), proving the recovery verifier to each.
func Recover(ctx context.Context, opts RecoverOptions) error {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if len(opts.Sources) == 0 {
		return errors.New("node: recover needs at least one source")
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

	var sessions []*session.Session
	defer func() {
		for _, s := range sessions {
			_ = s.Close()
		}
	}()

	var members, holders []*session.Session
	for _, src := range opts.Sources {
		sess, err := dialAndProve(ctx, log, ladder, opts, src)
		if err != nil {
			log.Warn("node: recover source unavailable", "source", src, "err", err)
			continue
		}
		sessions = append(sessions, sess)
		if !sessionShares(sess, opts.ShareID) {
			log.Warn("node: source does not serve the folder (or verifier mismatch)", "source", src)
			continue
		}
		if sess.PeerServesAsHolder(opts.ShareID) {
			holders = append(holders, sess)
		} else {
			members = append(members, sess)
		}
	}

	if len(members) > 0 {
		if err := recoverViaEngine(ctx, log, opts, members); err == nil {
			return nil
		} else {
			log.Warn("node: recovery from members failed, trying holders", "err", err)
		}
	}
	for _, sess := range holders {
		if err := recoverFromHolder(ctx, log, opts, sess); err == nil {
			return nil
		} else {
			log.Warn("node: recovery from holder failed", "peer", sess.PeerNodeID(), "err", err)
		}
	}
	return fmt.Errorf("node: no source could serve folder %s", opts.ShareID)
}

// dialAndProve handshakes with a source, advertising the verifier so it shares the folder.
func dialAndProve(ctx context.Context, log *slog.Logger, ladder *peermgr.Ladder, opts RecoverOptions, source string) (*session.Session, error) {
	conn, err := ladder.Connect(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	verifier := crypto.FolderVerifier(opts.Secret, opts.ShareID)
	sess, err := session.Handshake(ctx, session.Config{
		Conn:      conn,
		Initiator: true,
		Local: session.Local{
			NodeID:        opts.NodeID,
			Folders:       []session.Folder{{ShareID: opts.ShareID, EncryptionVerifier: verifier}},
			ClientName:    "trove",
			ClientVersion: "m6",
		},
		Authorize: func(context.Context, string) ([]string, bool, error) { return []string{opts.ShareID}, true, nil },
		Logger:    log,
	})
	if err != nil {
		return nil, fmt.Errorf("handshake: %w", err) // Handshake closes conn on error
	}
	return sess, nil
}

// recoverViaEngine pulls from member sources via the sync engine, staging in a temp store and
// materializing into Root. It returns once a source's root is reached.
func recoverViaEngine(ctx context.Context, log *slog.Logger, opts RecoverOptions, sessions []*session.Session) error {
	stage, err := os.MkdirTemp("", "trove-recover-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stage) }()
	mdb, err := storage.Open(storage.Options{Path: filepath.Join(stage, "model.db"), MaxOpenConns: 4})
	if err != nil {
		return err
	}
	defer func() { _ = mdb.Close() }()
	mdl, err := model.Open(model.Options{DB: mdb, NodeID: opts.NodeID})
	if err != nil {
		return err
	}
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

	// The engine serves plaintext, so the staging store needs no key.
	fc := chunkstore.FolderContext{}
	coord := syncengine.NewCoordinator(opts.ShareID, fc, chunks, 0, log)

	converged := make(chan struct{})
	var once sync.Once
	onConverged := func(string, snapshot.Root, uint64, int64) { once.Do(func() { close(converged) }) }

	pctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()

	started := 0
	for _, sess := range sessions {
		eng, err := syncengine.New(syncengine.Options{
			Session: sess, Logger: log, OnConverged: onConverged,
			Folders: []syncengine.FolderConfig{{
				FolderID: opts.ShareID, Role: syncengine.RoleReader, Root: opts.Root,
				FolderCtx: fc, Model: mdl, Chunks: chunks, Coord: coord,
			}},
		})
		if err != nil {
			log.Warn("node: recovery engine", "peer", sess.PeerNodeID(), "err", err)
			continue
		}
		sess.SetControlHandler(eng.Handle)
		wg.Add(2)
		go func(s *session.Session) { defer wg.Done(); _ = s.Run(pctx) }(sess)
		go func(e *syncengine.Engine) { defer wg.Done(); _ = e.Drive(pctx) }(eng)
		started++
	}
	if started == 0 {
		return errors.New("no usable member source")
	}
	select {
	case <-converged:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// recoverFromHolder fetches the sealed folder from a holder source and decrypts it into Root.
func recoverFromHolder(ctx context.Context, log *slog.Logger, opts RecoverOptions, sess *session.Session) error {
	stage, err := os.MkdirTemp("", "trove-recover-*")
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

	pctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	// No-op handler: an unexpected control message must not tear the session down mid-fetch.
	sess.SetControlHandler(func(context.Context, wire.MessageType, proto.Message) error { return nil })
	wg.Add(1)
	go func() { defer wg.Done(); _ = sess.Run(pctx) }()

	fc := chunkstore.FolderContext{Encrypted: true, MasterKey: opts.Secret}
	get := holder.GetBlobOverConn(sess.Conn(), opts.ShareID)
	if err := holder.Restore(ctx, opts.Secret, chunks, fc, opts.Root, get); err != nil {
		return fmt.Errorf("blob restore: %w", err)
	}
	return nil
}

func sessionShares(sess *session.Session, shareID string) bool {
	for _, id := range sess.SharedFolders() {
		if id == shareID {
			return true
		}
	}
	return false
}
