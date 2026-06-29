//go:build linux

package chunkstore

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// clone makes dst a whole-file copy-on-write clone of src via the FICLONE ioctl.
// dst must not exist.
func clone(src, dst string) error {
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()
	d, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	switch err := unix.IoctlFileClone(int(d.Fd()), int(s.Fd())); {
	case err == nil:
		return d.Close()
	case errors.Is(err, unix.ENOTSUP), errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.EXDEV), errors.Is(err, unix.ENOTTY), errors.Is(err, unix.EINVAL):
		// EINVAL covers an immutable or otherwise unclonable source; fall back to a copy.
		_ = d.Close()
		_ = os.Remove(dst)
		return errReflinkUnsupported
	default:
		_ = d.Close()
		_ = os.Remove(dst)
		return fmt.Errorf("chunkstore: ficlone: %w", err)
	}
}
