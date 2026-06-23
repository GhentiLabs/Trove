//go:build !unix

package scanner

import "io/fs"

// inode is unavailable off Unix; the fast path falls back to mtime+size+mode.
func inode(_ fs.FileInfo) uint64 { return 0 }
