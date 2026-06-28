package holder

import (
	"context"
	"fmt"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// catalogLabel identifies the sealed catalog blob: the holder stores it under
// BlindID(masterKey, catalogLabel), and it is sealed with a non-convergent random nonce
// because its content is mutable under this fixed id.
const catalogLabel = "trove/holder/catalog/v1"

// PutBlob stores one opaque blob under its blinded id on a holder.
type PutBlob func(ctx context.Context, blinded [crypto.BlindLen]byte, data []byte) error

// GetBlob fetches one opaque blob by its blinded id from a holder.
type GetBlob func(ctx context.Context, blinded [crypto.BlindLen]byte) ([]byte, error)

// Export seals a folder's live manifests and unique chunks and pushes them to a holder
// as blinded blobs: one catalog blob plus one blob per chunk. The holder receives only
// ciphertext under blinded ids; it never learns the key, names, paths, or content.
func Export(ctx context.Context, master [crypto.MasterKeyLen]byte, m *model.Store, chunks *chunkstore.Store, fc chunkstore.FolderContext, put PutBlob) error {
	records, err := m.ListManifests(ctx)
	if err != nil {
		return fmt.Errorf("holder: list manifests: %w", err)
	}
	live := make([]manifest.Manifest, 0, len(records))
	for _, r := range records {
		if !r.Deleted {
			live = append(live, r.Manifest)
		}
	}

	catalog := EncodeCatalog(live)
	if uint32(len(catalog)+crypto.MutableOverhead) > MaxBlobBytes {
		return fmt.Errorf("holder: catalog too large (%d live manifests, %d bytes exceeds %d limit)", len(live), len(catalog), MaxBlobBytes)
	}
	sealedCatalog, err := crypto.SealMutable(master, catalogLabel, catalog)
	if err != nil {
		return fmt.Errorf("holder: seal catalog: %w", err)
	}
	if err := put(ctx, crypto.BlindID(master, []byte(catalogLabel)), sealedCatalog); err != nil {
		return err
	}

	seen := make(map[hasher.ChunkID]struct{})
	for _, mf := range live {
		for _, c := range mf.Chunks {
			if _, ok := seen[c.ID]; ok {
				continue
			}
			seen[c.ID] = struct{}{}
			plaintext, err := chunks.Get(ctx, fc, c.ID)
			if err != nil {
				return fmt.Errorf("holder: read chunk %s: %w", c.ID, err)
			}
			sealed, err := crypto.Seal(master, c.ID, plaintext)
			if err != nil {
				return fmt.Errorf("holder: seal chunk %s: %w", c.ID, err)
			}
			if err := put(ctx, crypto.BlindID(master, c.ID[:]), sealed); err != nil {
				return err
			}
		}
	}
	return nil
}
