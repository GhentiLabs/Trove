package holder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// errChunkMismatch means a restored chunk's plaintext did not hash to its expected id.
var errChunkMismatch = errors.New("holder: restored chunk failed verification")

// Restore rebuilds a folder's plaintext tree under root from a holder's blinded blobs.
func Restore(ctx context.Context, master [crypto.MasterKeyLen]byte, chunks *chunkstore.Store, fc chunkstore.FolderContext, root string, get GetBlob) error {
	sealedCatalog, err := get(ctx, crypto.BlindID(master, []byte(catalogLabel)))
	if err != nil {
		return fmt.Errorf("holder: fetch catalog: %w", err)
	}
	catalogBytes, err := crypto.OpenMutable(master, catalogLabel, sealedCatalog)
	if err != nil {
		return fmt.Errorf("holder: open catalog: %w", err)
	}
	manifests, err := DecodeCatalog(catalogBytes)
	if err != nil {
		return err
	}

	if err := restoreChunks(ctx, master, chunks, fc, manifests, get); err != nil {
		return err
	}
	return materialize(ctx, chunks, fc, root, manifests)
}

func restoreChunks(ctx context.Context, master [crypto.MasterKeyLen]byte, chunks *chunkstore.Store, fc chunkstore.FolderContext, manifests []manifest.Manifest, get GetBlob) error {
	seen := make(map[hasher.ChunkID]struct{})
	for _, mf := range manifests {
		for _, c := range mf.Chunks {
			if _, ok := seen[c.ID]; ok {
				continue
			}
			seen[c.ID] = struct{}{}
			sealed, err := get(ctx, crypto.BlindID(master, c.ID[:]))
			if err != nil {
				return fmt.Errorf("holder: fetch chunk %s: %w", c.ID, err)
			}
			plaintext, err := crypto.Open(master, c.ID, sealed)
			if err != nil {
				return fmt.Errorf("holder: open chunk %s: %w", c.ID, err)
			}
			if hasher.Sum(plaintext) != c.ID {
				return fmt.Errorf("%w: %s", errChunkMismatch, c.ID)
			}
			if _, err := chunks.Put(ctx, fc, plaintext); err != nil {
				return fmt.Errorf("holder: store chunk %s: %w", c.ID, err)
			}
		}
	}
	return nil
}

func materialize(ctx context.Context, chunks *chunkstore.Store, fc chunkstore.FolderContext, root string, manifests []manifest.Manifest) error {
	// Validate every manifest before touching the filesystem.
	for _, mf := range manifests {
		if err := model.ValidateManifest(mf); err != nil {
			return fmt.Errorf("holder: reject manifest %q: %w", mf.Path, err)
		}
	}
	sorted := append([]manifest.Manifest(nil), manifests...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	for _, mf := range sorted {
		dest, err := safeJoin(root, mf.Path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return fmt.Errorf("holder: parent dir: %w", err)
		}
		switch mf.Kind {
		case manifest.KindDir:
			if err := os.MkdirAll(dest, os.FileMode(mf.Mode)&os.ModePerm); err != nil {
				return fmt.Errorf("holder: mkdir %q: %w", mf.Path, err)
			}
		case manifest.KindSymlink:
			_ = os.Remove(dest)
			if err := os.Symlink(mf.SymlinkTarget, dest); err != nil {
				return fmt.Errorf("holder: symlink %q: %w", mf.Path, err)
			}
		case manifest.KindRegular:
			if err := writeRegular(ctx, chunks, fc, dest, mf); err != nil {
				return err
			}
		default:
			return fmt.Errorf("holder: unknown kind %d for %q", mf.Kind, mf.Path)
		}
	}
	return nil
}

func writeRegular(ctx context.Context, chunks *chunkstore.Store, fc chunkstore.FolderContext, dest string, mf manifest.Manifest) error {
	ids := make([]hasher.ChunkID, len(mf.Chunks))
	for i, c := range mf.Chunks {
		ids[i] = c.ID
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".restore-*")
	if err != nil {
		return fmt.Errorf("holder: temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := chunks.Reassemble(ctx, fc, ids, tmp); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("holder: reassemble %q: %w", mf.Path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("holder: close %q: %w", mf.Path, err)
	}
	if err := os.Chmod(tmpName, os.FileMode(mf.Mode)&os.ModePerm); err != nil {
		return fmt.Errorf("holder: chmod %q: %w", mf.Path, err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("holder: rename %q: %w", mf.Path, err)
	}
	return nil
}

func safeJoin(root, rel string) (string, error) {
	dest := filepath.Join(root, filepath.FromSlash(rel))
	rootClean := filepath.Clean(root)
	if dest == rootClean || !strings.HasPrefix(dest, rootClean+string(os.PathSeparator)) {
		return "", fmt.Errorf("holder: path %q escapes root", rel)
	}
	return dest, nil
}
