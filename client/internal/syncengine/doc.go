// Package syncengine drives one-way folder convergence over an Active session: an
// owner serves its manifests and chunks; a replica pulls them and applies them
// crash-safely to disk. One Engine is bound to one session and covers the folders
// shared on it. It depends on model, chunkstore, session, and wire, and nothing
// depends back on it.
//
// Control messages (FolderSummary, ManifestRequest, ManifestDelta) ride the session
// control stream as protobuf; chunk payloads ride dedicated data streams as raw
// bytes behind the frozen header in codec.go.
package syncengine
