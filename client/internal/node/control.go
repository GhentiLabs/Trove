package node

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/GhentiLabs/Trove/client/internal/config"
	"github.com/GhentiLabs/Trove/client/internal/control"
	"github.com/GhentiLabs/Trove/client/internal/membership"
	"github.com/GhentiLabs/Trove/client/internal/model"
	"github.com/GhentiLabs/Trove/client/internal/peermgr"
	"github.com/GhentiLabs/Trove/client/internal/syncengine"
)

const controlShutdownTimeout = 2 * time.Second

// listenControl binds the control socket, treating a live socket as another
// daemon owning the state dir and a dead one as stale.
func (s *Service) listenControl() (net.Listener, string, error) {
	path := control.SocketPath(s.opts.StateDir)
	if _, err := os.Stat(path); err == nil {
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			_ = conn.Close()
			return nil, "", fmt.Errorf("node: another daemon is already running on %s", s.opts.StateDir)
		}
		if err := os.Remove(path); err != nil {
			return nil, "", fmt.Errorf("node: remove stale control socket: %w", err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, "", fmt.Errorf("node: control socket %q (a long state dir path exceeds the unix socket limit): %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, "", fmt.Errorf("node: control socket permissions: %w", err)
	}
	return ln, path, nil
}

// controlBackend implements control.Backend over a running Service.
type controlBackend struct{ s *Service }

func (b controlBackend) Identity(context.Context) (control.Identity, error) {
	key, ok := b.s.opts.Cert.PrivateKey.(ed25519.PrivateKey)
	if !ok {
		return control.Identity{}, errors.New("node: certificate key is not Ed25519")
	}
	return control.Identity{
		NodeID:    b.s.opts.NodeID,
		PublicKey: hex.EncodeToString(key.Public().(ed25519.PublicKey)),
	}, nil
}

func (b controlBackend) Status(ctx context.Context) (control.Status, error) {
	folders, err := b.s.opts.Config.ListFolders(ctx)
	if err != nil {
		return control.Status{}, err
	}
	st := control.Status{NodeID: b.s.opts.NodeID}
	err = b.s.withRuntime(func(rt *syncRuntime, _ *peermgr.Manager) error {
		for _, f := range folders {
			if f.ShareID == "" {
				continue
			}
			cf, err := b.folderStatus(ctx, rt, f)
			if err != nil {
				return err
			}
			st.Folders = append(st.Folders, cf)
		}
		return nil
	})
	return st, err
}

// folderStatus renders one folder, reading usage and receipts from the live
// runtime when the folder's stores are attached. Callers hold the runtime lock.
func (b controlBackend) folderStatus(ctx context.Context, rt *syncRuntime, f config.Folder) (control.Folder, error) {
	cf := control.Folder{
		ID: f.ShareID, Role: b.roleOf(ctx, f), Root: f.Root,
		Encrypted: f.Encrypted, Holder: f.Holder, KeepHistory: f.KeepHistory, QuotaBytes: f.QuotaBytes,
	}
	fc := runtimeFolder(rt, f.ShareID)
	if fc == nil {
		return cf, nil
	}
	used, err := fc.Model.ReachableLogicalBytes(ctx)
	if err != nil {
		return control.Folder{}, err
	}
	receipts, err := fc.Model.Receipts(ctx, model.LocalSync)
	if err != nil {
		return control.Folder{}, err
	}
	cf.Synced = true
	cf.UsedBytes = used
	for _, r := range receipts {
		cf.Receipts = append(cf.Receipts, control.Receipt{PeerID: r.PeerID, HighWater: r.HighWater, SyncedAt: r.SyncedAt})
	}
	return cf, nil
}

func runtimeFolder(rt *syncRuntime, shareID string) *syncengine.FolderConfig {
	if rt == nil {
		return nil
	}
	for i := range rt.folders {
		if rt.folders[i].FolderID == shareID {
			return &rt.folders[i]
		}
	}
	return nil
}

func (b controlBackend) roleOf(ctx context.Context, f config.Folder) string {
	if f.Holder {
		return "holder"
	}
	if founder, ok := membership.Founder(f.ShareID); ok && founder == b.s.opts.NodeID {
		return "owner"
	}
	if role, ok, err := b.s.members.RoleOf(ctx, f.ShareID, b.s.opts.NodeID); err == nil && ok && role == membership.RoleWriter {
		return "writer"
	}
	return "reader"
}

func (b controlBackend) Peers(context.Context) (control.Peers, error) {
	var out control.Peers
	err := b.s.withRuntime(func(_ *syncRuntime, mgr *peermgr.Manager) error {
		if mgr == nil {
			return nil
		}
		for _, sess := range mgr.Sessions() {
			out.Active = append(out.Active, control.Peer{NodeID: sess.PeerNodeID(), Folders: sess.SharedFolders()})
		}
		return nil
	})
	return out, err
}

func (b controlBackend) Found(ctx context.Context, req control.FoundRequest) (control.FoundResponse, error) {
	if req.Root == "" {
		return control.FoundResponse{}, fmt.Errorf("%w: root is required", control.ErrInvalid)
	}
	b.s.cfgMu.Lock()
	defer b.s.cfgMu.Unlock()
	if err := b.checkFolderMix(ctx, false); err != nil {
		return control.FoundResponse{}, err
	}
	group, err := b.s.members.Found(ctx)
	if err != nil {
		return control.FoundResponse{}, err
	}
	folder := config.Folder{
		ID: group, Root: req.Root, ShareID: group,
		Encrypted: req.Encrypted, KeepHistory: req.KeepHistory, QuotaBytes: req.QuotaBytes,
	}
	if err := b.s.opts.Config.AddFolder(ctx, folder); err != nil {
		return control.FoundResponse{}, err
	}
	var secret [config.MasterKeyLen]byte
	if req.Encrypted {
		secret, err = b.s.opts.Config.GenerateFolderKey(ctx, group)
	} else {
		secret, err = b.s.opts.Config.GenerateRecoverySecret(ctx, group)
	}
	if err != nil {
		return control.FoundResponse{}, err
	}
	if err := b.s.requestReload(ctx); err != nil {
		return control.FoundResponse{}, err
	}
	return control.FoundResponse{GroupID: group, RecoveryCode: config.EncodeRecoveryCode(secret)}, nil
}

func (b controlBackend) Join(ctx context.Context, req control.JoinRequest) error {
	if _, ok := membership.Founder(req.GroupID); !ok {
		return fmt.Errorf("%w: %q is not a valid group id", control.ErrInvalid, req.GroupID)
	}
	if !req.Holder && req.Root == "" {
		return fmt.Errorf("%w: root is required", control.ErrInvalid)
	}
	b.s.cfgMu.Lock()
	defer b.s.cfgMu.Unlock()
	if err := b.checkFolderMix(ctx, req.Holder); err != nil {
		return err
	}
	if err := b.s.members.Join(ctx, req.GroupID); err != nil {
		return err
	}
	folder := config.Folder{
		ID: req.GroupID, Root: req.Root, ShareID: req.GroupID,
		Encrypted: req.Encrypted || req.Holder, Holder: req.Holder,
		KeepHistory: req.KeepHistory, QuotaBytes: req.QuotaBytes,
	}
	if err := b.s.opts.Config.AddFolder(ctx, folder); err != nil {
		if errors.Is(err, config.ErrFolderExists) {
			return fmt.Errorf("%w: folder %q", control.ErrExists, req.GroupID)
		}
		return err
	}
	return b.s.requestReload(ctx)
}

// checkFolderMix enforces buildSyncRuntime's invariant before a mutation commits,
// so a rebuild cannot fail (and exit the daemon) on a holder/sync mix.
func (b controlBackend) checkFolderMix(ctx context.Context, addingHolder bool) error {
	folders, err := b.s.opts.Config.ListFolders(ctx)
	if err != nil {
		return err
	}
	for _, f := range folders {
		if f.ShareID == "" {
			continue
		}
		if f.Holder != addingHolder {
			return fmt.Errorf("%w: a holder node cannot also sync folders; run a dedicated holder", control.ErrInvalid)
		}
	}
	return nil
}

func (b controlBackend) Invite(ctx context.Context, folderID string, req control.InviteRequest) error {
	if _, err := b.s.opts.Config.GetFolder(ctx, folderID); err != nil {
		if errors.Is(err, config.ErrFolderNotFound) {
			return fmt.Errorf("%w: folder %q", control.ErrNotFound, folderID)
		}
		return err
	}
	var role membership.Role
	switch req.Role {
	case "reader":
		role = membership.RoleReader
	case "writer":
		role = membership.RoleWriter
	case "holder":
		role = membership.RoleHolder
	default:
		return fmt.Errorf("%w: role %q is not reader, writer, or holder", control.ErrInvalid, req.Role)
	}
	pub, err := hex.DecodeString(req.PublicKey)
	if err != nil {
		return fmt.Errorf("%w: bad public key: %w", control.ErrInvalid, err)
	}
	if req.NodeID == "" {
		return fmt.Errorf("%w: node id is required", control.ErrInvalid)
	}
	_, err = b.s.members.Add(ctx, folderID, req.NodeID, pub, role)
	return err
}

func (b controlBackend) SetQuota(ctx context.Context, folderID string, quota int64) (control.Folder, error) {
	if quota < 0 {
		return control.Folder{}, fmt.Errorf("%w: quota must be >= 0", control.ErrInvalid)
	}
	b.s.cfgMu.Lock()
	defer b.s.cfgMu.Unlock()
	if err := b.s.opts.Config.SetFolderQuota(ctx, folderID, quota); err != nil {
		if errors.Is(err, config.ErrFolderNotFound) {
			return control.Folder{}, fmt.Errorf("%w: folder %q", control.ErrNotFound, folderID)
		}
		return control.Folder{}, err
	}
	f, err := b.s.opts.Config.GetFolder(ctx, folderID)
	if err != nil {
		return control.Folder{}, err
	}
	var out control.Folder
	err = b.s.withRuntime(func(rt *syncRuntime, _ *peermgr.Manager) error {
		if fc := runtimeFolder(rt, f.ShareID); fc != nil {
			fc.Model.SetQuota(quota)
			if err := fc.Model.PruneHistoryToFit(ctx); errors.Is(err, model.ErrQuotaExceeded) {
				out.QuotaWarning = "current data alone exceeds the quota; history was pruned but nothing more can be freed"
			} else if err != nil {
				return err
			}
		}
		cf, err := b.folderStatus(ctx, rt, f)
		if err != nil {
			return err
		}
		cf.QuotaWarning = out.QuotaWarning
		out = cf
		return nil
	})
	return out, err
}

func (b controlBackend) Remove(ctx context.Context, folderID string, purge bool) error {
	b.s.cfgMu.Lock()
	defer b.s.cfgMu.Unlock()
	if _, err := b.s.opts.Config.GetFolder(ctx, folderID); err != nil {
		if errors.Is(err, config.ErrFolderNotFound) {
			return fmt.Errorf("%w: folder %q", control.ErrNotFound, folderID)
		}
		return err
	}
	if err := b.s.opts.Config.RemoveFolder(ctx, folderID); err != nil {
		return err
	}
	// The reload ack means the old stack is torn down and the folder's stores are
	// closed, so a purge deletes quiescent files.
	if err := b.s.requestReload(ctx); err != nil {
		return err
	}
	if purge {
		if err := os.RemoveAll(filepath.Join(b.s.opts.StateDir, "folders", folderID)); err != nil {
			return fmt.Errorf("node: purge folder state: %w", err)
		}
	}
	return nil
}
