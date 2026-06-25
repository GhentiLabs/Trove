package wire

import (
	"bytes"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/wire/wirepb"
)

// FuzzReadMessage feeds arbitrary bytes to the frame decoder: it must never panic or
// over-allocate, only return a clean error.
func FuzzReadMessage(f *testing.F) {
	var buf bytes.Buffer
	_ = WriteMessage(&buf, &wirepb.Ping{})
	f.Add(buf.Bytes())
	buf.Reset()
	_ = WriteMessage(&buf, &wirepb.NetworkConfig{Folders: []*wirepb.Folder{{FolderId: "x"}}})
	f.Add(buf.Bytes())
	buf.Reset()
	_ = WriteMessage(&buf, &wirepb.Close{Reason: "bye"})
	f.Add(buf.Bytes())
	buf.Reset()
	_ = WriteMessage(&buf, &wirepb.FolderSummary{FolderId: "x", SnapshotRoot: make([]byte, 32), IndexEpochId: 1})
	f.Add(buf.Bytes())
	buf.Reset()
	_ = WriteMessage(&buf, &wirepb.ManifestRequest{FolderId: "x", IndexEpochId: 1, SinceSequence: 3})
	f.Add(buf.Bytes())
	buf.Reset()
	_ = WriteMessage(&buf, &wirepb.ManifestDelta{FolderId: "x", Manifests: []*wirepb.RemoteManifest{{Path: "a"}}})
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ReadMessage(bytes.NewReader(data))
	})
}

// FuzzReadHello feeds arbitrary bytes to the Hello decoder; same contract.
func FuzzReadHello(f *testing.F) {
	var buf bytes.Buffer
	_ = WriteHello(&buf, &wirepb.Hello{NodeId: "x", WireFormatVersion: WireFormatVersion})
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x54, 0x52, 0x4f, 0x56}) // bare magic, no length/body

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadHello(bytes.NewReader(data))
	})
}
