package holder

import (
	"context"
	"fmt"
	"sync"

	"github.com/GhentiLabs/Trove/client/internal/chunkstore"
	"github.com/GhentiLabs/Trove/client/internal/crypto"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
	"github.com/GhentiLabs/Trove/client/internal/manifest"
	"github.com/GhentiLabs/Trove/client/internal/model"
)

// pointerLabel identifies the sealed catalog pointer, stored under a fixed blinded id. Its
// content (the current catalog's id) changes, so it is sealed with a random nonce
// (SealMutable). The catalog itself is content-addressed and convergently sealed.
const pointerLabel = "trove/holder/catalog-pointer/v1"

// PutBlob stores one opaque blob under its blinded id on a holder.
type PutBlob func(ctx context.Context, blinded [crypto.BlindIDLen]byte, data []byte) error

// GetBlob fetches one opaque blob by its blinded id from a holder.
type GetBlob func(ctx context.Context, blinded [crypto.BlindIDLen]byte) ([]byte, error)

// HasBlobs reports, for up to MaxHasBatch ids, which the holder already stores.
type HasBlobs func(ctx context.Context, ids [][crypto.BlindIDLen]byte) ([]bool, error)

// Reconcile brings a holder up to date with a folder's current live tree, pushing only the
// blobs the holder does not already have: the unique chunks (content-addressed, sealed under
// blinded ids), then the content-addressed catalog, then the pointer that commits the new
// version. The holder receives only ciphertext under blinded ids — never the key, names,
// paths, or content. Chunks and catalog before the pointer means an interrupted push leaves
// the holder's previous consistent version intact.
func Reconcile(ctx context.Context, master [crypto.MasterKeyLen]byte, m *model.Store, chunks *chunkstore.Store, fc chunkstore.FolderContext, has HasBlobs, put PutBlob) error {
	records, err := m.ListLiveManifests(ctx)
	if err != nil {
		return fmt.Errorf("holder: list manifests: %w", err)
	}
	live := make([]manifest.Manifest, len(records))
	for i, r := range records {
		live[i] = r.Manifest
	}

	catalog := EncodeCatalog(live)
	if uint32(len(catalog)+crypto.SealOverhead) > MaxBlobBytes {
		return fmt.Errorf("holder: catalog too large (%d live manifests, %d bytes exceeds %d limit)", len(live), len(catalog), MaxBlobBytes)
	}
	catalogID := hasher.Sum(catalog)

	chunkIDs := uniqueChunks(live)
	if err := pushMissingChunks(ctx, master, chunks, fc, chunkIDs, has, put); err != nil {
		return err
	}

	catalogBlind := crypto.BlindID(master, catalogID[:])
	switch present, err := has(ctx, [][crypto.BlindIDLen]byte{catalogBlind}); {
	case err != nil:
		return err
	case len(present) == 0 || !present[0]:
		sealed, err := crypto.Seal(master, catalogID, catalog)
		if err != nil {
			return fmt.Errorf("holder: seal catalog: %w", err)
		}
		if err := put(ctx, catalogBlind, sealed); err != nil {
			return err
		}
	}

	sealedPointer, err := crypto.SealMutable(master, pointerLabel, catalogID[:])
	if err != nil {
		return fmt.Errorf("holder: seal pointer: %w", err)
	}
	return put(ctx, crypto.BlindID(master, []byte(pointerLabel)), sealedPointer)
}

func uniqueChunks(live []manifest.Manifest) []hasher.ChunkID {
	seen := make(map[hasher.ChunkID]struct{})
	var ids []hasher.ChunkID
	for _, mf := range live {
		for _, c := range mf.Chunks {
			if _, ok := seen[c.ID]; ok {
				continue
			}
			seen[c.ID] = struct{}{}
			ids = append(ids, c.ID)
		}
	}
	return ids
}

// pushConcurrency bounds how many chunk blobs are sealed and pushed at once.
const pushConcurrency = 16

func pushMissingChunks(ctx context.Context, master [crypto.MasterKeyLen]byte, chunks *chunkstore.Store, fc chunkstore.FolderContext, chunkIDs []hasher.ChunkID, has HasBlobs, put PutBlob) error {
	blinded := make([][crypto.BlindIDLen]byte, len(chunkIDs))
	for i, id := range chunkIDs {
		blinded[i] = crypto.BlindID(master, id[:])
	}
	present, err := hasAll(ctx, has, blinded)
	if err != nil {
		return err
	}
	var absent []int
	for i := range chunkIDs {
		if !present[i] {
			absent = append(absent, i)
		}
	}
	return parallelDo(ctx, pushConcurrency, absent, func(ctx context.Context, i int) error {
		id := chunkIDs[i]
		plaintext, err := chunks.Get(ctx, fc, id)
		if err != nil {
			return fmt.Errorf("holder: read chunk %s: %w", id, err)
		}
		sealed, err := crypto.Seal(master, id, plaintext)
		if err != nil {
			return fmt.Errorf("holder: seal chunk %s: %w", id, err)
		}
		return put(ctx, blinded[i], sealed)
	})
}

// parallelDo runs fn over each index with at most limit in flight, returning the first error
// and cancelling the rest.
func parallelDo(ctx context.Context, limit int, indices []int, fn func(context.Context, int) error) error {
	if len(indices) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, i := range indices {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			if err := fn(ctx, i); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return firstErr
}

// hasConcurrency bounds how many has-batch round-trips run at once.
const hasConcurrency = 8

// hasAll asks the holder which ids it holds, in concurrent MaxHasBatch-sized batches.
func hasAll(ctx context.Context, has HasBlobs, ids [][crypto.BlindIDLen]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	batches := make([]int, (len(ids)+MaxHasBatch-1)/MaxHasBatch)
	for i := range batches {
		batches[i] = i * MaxHasBatch
	}
	err := parallelDo(ctx, hasConcurrency, batches, func(ctx context.Context, start int) error {
		end := min(start+MaxHasBatch, len(ids))
		present, err := has(ctx, ids[start:end])
		if err != nil {
			return err
		}
		if len(present) != end-start {
			return fmt.Errorf("holder: has-batch returned %d of %d", len(present), end-start)
		}
		copy(out[start:end], present)
		return nil
	})
	return out, err
}
