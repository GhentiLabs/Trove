//go:build darwin

package chunkstore

import (
	"errors"

	"golang.org/x/sys/unix"
)

// clone makes dst a whole-file copy-on-write clone of src via clonefile(2). dst
// must not exist.
func clone(src, dst string) error {
	switch err := unix.Clonefile(src, dst, 0); {
	case err == nil:
		return nil
	case errors.Is(err, unix.ENOTSUP), errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.EXDEV):
		return errReflinkUnsupported
	default:
		return err
	}
}
