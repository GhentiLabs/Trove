package holder

import (
	"context"
	"fmt"

	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// Collect reclaims a holder's blobs that the folder's current version no longer references
// — superseded catalogs and chunks of deleted files. It pages the holder's blobs, keeps the
// pointer, the live catalog, and every live chunk, and deletes the rest, skipping any blob
// written within graceMillis so a concurrent writer's in-flight push is never reaped (the
// holder cannot self-collect: it can't read the encrypted catalog). A wrongly-reaped blob
// self-heals on the next reconcile.
func Collect(ctx context.Context, master [crypto.MasterKeyLen]byte, m *model.Store, list ListBlobs, del DeleteBlobs, graceMillis, nowMillis int64) error {
	records, err := m.ListLiveManifests(ctx)
	if err != nil {
		return fmt.Errorf("holder: list manifests: %w", err)
	}
	live := make([]manifest.Manifest, len(records))
	for i, r := range records {
		live[i] = r.Manifest
	}

	catalogID := hasher.Sum(EncodeCatalog(live))
	needed := map[[crypto.BlindIDLen]byte]struct{}{
		crypto.BlindID(master, []byte(pointerLabel)): {},
		crypto.BlindID(master, catalogID[:]):         {},
	}
	for _, id := range uniqueChunks(live) {
		needed[crypto.BlindID(master, id[:])] = struct{}{}
	}

	var after [crypto.BlindIDLen]byte
	var stale [][crypto.BlindIDLen]byte
	for {
		page, err := list(ctx, after)
		if err != nil {
			return err
		}
		for _, ref := range page {
			if _, keep := needed[ref.ID]; keep {
				continue
			}
			if nowMillis-ref.ModMillis < graceMillis {
				continue
			}
			stale = append(stale, ref.ID)
		}
		if len(page) < MaxListPage {
			break
		}
		after = page[len(page)-1].ID
	}

	for start := 0; start < len(stale); start += MaxHasBatch {
		end := min(start+MaxHasBatch, len(stale))
		if err := del(ctx, stale[start:end]); err != nil {
			return err
		}
	}
	return nil
}
