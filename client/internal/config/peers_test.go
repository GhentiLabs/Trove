package config

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

const peerA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
const peerB = "cccccccccccccccccccccccccccccccccccccccccccccccccccc"

func TestPeerCRUD(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	mustShareFolder(t, s, "docs", "docs-share")
	mustShareFolder(t, s, "photos", "photos-share")

	want := Peer{NodeID: peerA, Name: "laptop", Folders: []string{"docs-share", "photos-share"}}
	if err := s.AddPeer(ctx, want); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if err := s.AddPeer(ctx, want); !errors.Is(err, ErrPeerExists) {
		t.Fatalf("duplicate AddPeer err = %v, want ErrPeerExists", err)
	}

	got, err := s.GetPeer(ctx, peerA)
	if err != nil {
		t.Fatalf("GetPeer: %v", err)
	}
	if got.NodeID != want.NodeID || got.Name != want.Name {
		t.Fatalf("GetPeer = %+v, want %+v", got, want)
	}
	if len(got.Folders) != 2 || got.Folders[0] != "docs-share" || got.Folders[1] != "photos-share" {
		t.Fatalf("GetPeer folders = %v", got.Folders)
	}

	if _, err := s.GetPeer(ctx, peerB); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("GetPeer(missing) err = %v, want ErrPeerNotFound", err)
	}

	if err := s.AddPeer(ctx, Peer{NodeID: peerB, Name: "phone"}); err != nil {
		t.Fatalf("AddPeer phone: %v", err)
	}
	list, err := s.ListPeers(ctx)
	if err != nil {
		t.Fatalf("ListPeers: %v", err)
	}
	if len(list) != 2 || list[0].NodeID != peerA || list[1].NodeID != peerB {
		t.Fatalf("ListPeers = %+v", list)
	}

	if err := s.RemovePeer(ctx, peerA); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	if err := s.RemovePeer(ctx, peerA); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("RemovePeer(missing) err = %v, want ErrPeerNotFound", err)
	}
	if _, err := s.GetPeer(ctx, peerA); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("GetPeer after remove err = %v, want ErrPeerNotFound", err)
	}
}

func TestAddPeerRejectsInvalidNodeID(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddPeer(ctx, Peer{NodeID: "too-short"}); !errors.Is(err, ErrInvalidNodeID) {
		t.Fatalf("AddPeer(invalid) err = %v, want ErrInvalidNodeID", err)
	}
}

func TestAddPeerRejectsUnknownShareID(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)

	if err := s.AddPeer(ctx, Peer{NodeID: peerA, Folders: []string{"never-paired"}}); !errors.Is(err, ErrUnknownShareID) {
		t.Fatalf("AddPeer(unknown share) err = %v, want ErrUnknownShareID", err)
	}
	if err := s.AddPeer(ctx, Peer{NodeID: peerA, Folders: []string{""}}); !errors.Is(err, ErrUnknownShareID) {
		t.Fatalf("AddPeer(empty share) err = %v, want ErrUnknownShareID", err)
	}
	if _, err := s.GetPeer(ctx, peerA); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("rejected peer must not be persisted, GetPeer err = %v", err)
	}
}

func TestRemoveFolderPrunesGrants(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	mustShareFolder(t, s, "docs", "docs-share")
	if err := s.AddPeer(ctx, Peer{NodeID: peerA, Folders: []string{"docs-share"}}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if err := s.RemoveFolder(ctx, "docs"); err != nil {
		t.Fatalf("RemoveFolder: %v", err)
	}
	p, err := s.GetPeer(ctx, peerA)
	if err != nil {
		t.Fatalf("GetPeer: %v", err)
	}
	if len(p.Folders) != 0 {
		t.Fatalf("grant not pruned after folder removed: %v", p.Folders)
	}
}

func TestRotateShareIDPrunesOldGrant(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	mustShareFolder(t, s, "docs", "docs-share")
	if err := s.AddPeer(ctx, Peer{NodeID: peerA, Folders: []string{"docs-share"}}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if err := s.SetFolderShareID(ctx, "docs", "rotated-share"); err != nil {
		t.Fatalf("SetFolderShareID: %v", err)
	}
	p, err := s.GetPeer(ctx, peerA)
	if err != nil {
		t.Fatalf("GetPeer: %v", err)
	}
	if len(p.Folders) != 0 {
		t.Fatalf("stale grant not pruned after share id rotation: %v", p.Folders)
	}
}

func mustShareFolder(t *testing.T, s *Store, id, shareID string) {
	t.Helper()
	ctx := context.Background()
	if err := s.AddFolder(ctx, Folder{ID: id, Root: "/" + id}); err != nil {
		t.Fatalf("AddFolder %s: %v", id, err)
	}
	if err := s.SetFolderShareID(ctx, id, shareID); err != nil {
		t.Fatalf("SetFolderShareID %s: %v", id, err)
	}
}

func TestRemovePeerClearsFolders(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	mustShareFolder(t, s, "fx", "x")
	mustShareFolder(t, s, "fy", "y")
	if err := s.AddPeer(ctx, Peer{NodeID: peerA, Folders: []string{"x", "y"}}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if err := s.RemovePeer(ctx, peerA); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}
	var n int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM peer_folders WHERE node_id = ?`, peerA).Scan(&n); err != nil {
		t.Fatalf("count peer_folders: %v", err)
	}
	if n != 0 {
		t.Fatalf("peer_folders rows after remove = %d, want 0", n)
	}
}

func TestFolderShareID(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, openDB(t, filepath.Join(t.TempDir(), "c.db")), testNode)
	if err := s.AddFolder(ctx, Folder{ID: "docs", Root: "/d"}); err != nil {
		t.Fatalf("AddFolder: %v", err)
	}

	got, err := s.GetFolder(ctx, "docs")
	if err != nil {
		t.Fatalf("GetFolder: %v", err)
	}
	if got.ShareID != "" {
		t.Fatalf("new folder ShareID = %q, want empty", got.ShareID)
	}

	if err := s.SetFolderShareID(ctx, "docs", "docs-share"); err != nil {
		t.Fatalf("SetFolderShareID: %v", err)
	}
	got, err = s.GetFolder(ctx, "docs")
	if err != nil {
		t.Fatalf("GetFolder after set: %v", err)
	}
	if got.ShareID != "docs-share" {
		t.Fatalf("ShareID = %q, want docs-share", got.ShareID)
	}

	if err := s.SetFolderShareID(ctx, "missing", "x"); !errors.Is(err, ErrFolderNotFound) {
		t.Fatalf("SetFolderShareID(missing) err = %v, want ErrFolderNotFound", err)
	}
}
