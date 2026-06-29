package chunkstore

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// errReflinkUnsupported signals that the platform or filesystem cannot make a
// copy-on-write clone, so the caller falls back to a physical copy.
var errReflinkUnsupported = errors.New("chunkstore: reflink unsupported")

// physicalCopy writes src to a new file at dst byte-for-byte. dst must not exist.
// It does not fsync; the caller is responsible for durability.
func physicalCopy(src, dst string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	d, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(d, s); err != nil {
		_ = d.Close()
		_ = os.Remove(dst)
		return fmt.Errorf("chunkstore: copy: %w", err)
	}
	return d.Close()
}

// cloneOrCopy makes dst a copy-on-write clone of src, falling back to a physical
// copy when the platform or filesystem cannot clone. dst must not exist. It
// reports whether a real clone was made (false means a physical copy was used).
// The destination is not fsynced; the caller must do so before relying on it.
func cloneOrCopy(src, dst string) (cloned bool, err error) {
	return cloneOrCopyWith(clone, src, dst)
}

// cloneOrCopyWith is cloneOrCopy with the clone primitive injected, so a test can
// exercise the physical-copy fallback on a filesystem that does support cloning.
func cloneOrCopyWith(cloneFn func(src, dst string) error, src, dst string) (cloned bool, err error) {
	switch err := cloneFn(src, dst); {
	case err == nil:
		return true, nil
	case errors.Is(err, errReflinkUnsupported):
		return false, physicalCopy(src, dst)
	default:
		return false, err
	}
}
