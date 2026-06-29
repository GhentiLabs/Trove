//go:build !darwin && !linux

package chunkstore

// clone is unsupported off darwin and linux, so callers fall back to a physical copy.
func clone(src, dst string) error {
	return errReflinkUnsupported
}
