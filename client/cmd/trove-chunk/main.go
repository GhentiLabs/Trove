// Command trove-chunk chunks a file and prints each chunk's offset, length, and
// identity. It is a manual aid for eyeballing chunk determinism, not part of the
// daemon.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/GhentiLabs/Trove/client/internal/chunker"
	"github.com/GhentiLabs/Trove/client/internal/hasher"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: trove-chunk <file>")
		os.Exit(2)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "trove-chunk:", err)
		os.Exit(1)
	}
}

func run(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	c := chunker.New(chunker.Options{Reader: f})
	var count, total int64
	for {
		ch, data, err := c.NextChunk()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("%6d  off=%-12d len=%-9d %s\n", count, ch.Offset, ch.Length, hasher.Sum(data))
		count++
		total += int64(ch.Length)
	}
	fmt.Printf("%d chunks, %d bytes\n", count, total)
	return nil
}
