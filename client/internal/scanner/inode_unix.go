//go:build unix

package scanner

import (
	"io/fs"
	"syscall"
)

// inode returns the file's inode number, or 0 if unavailable. It lets the stat
// fast path notice a file replaced in place with the same size, mode, and mtime.
func inode(fi fs.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}
