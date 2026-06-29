package syncengine

import (
	"context"
	"testing"

	"github.com/GhentiLabs/Trove/client/internal/session"
	"github.com/GhentiLabs/Trove/client/internal/transport"
	"github.com/GhentiLabs/Trove/pkg/identity"
)

func realTransport(t *testing.T) (*transport.Transport, string) {
	t.Helper()
	_, key, err := identity.GenerateKey()
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	cert, err := identity.NewCertificate(key)
	if err != nil {
		t.Fatalf("NewCertificate: %v", err)
	}
	id := identity.FingerprintCert(cert.Leaf)
	tr, err := transport.New(transport.Options{Cert: cert, UDPAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })
	return tr, id
}

// TestConvergeOverRealQUIC exercises the join the MemNet tests cannot: real buffered
// QUIC streams, stream FIN as transfer-complete, and real backpressure on loopback.
func TestConvergeOverRealQUIC(t *testing.T) {
	t.Parallel()
	ownerTr, ownerFP := realTransport(t)
	replicaTr, replicaFP := realTransport(t)

	owner := newPeer(t, ownerFP)
	replica := newPeer(t, replicaFP)
	writeFile(t, owner.root, "a.txt", []byte("over quic"))
	writeFile(t, owner.root, "dir/big.bin", pseudoRandom(2<<20, 7))
	owner.scan(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type res struct {
		s   *session.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		conn, err := ownerTr.Accept(ctx)
		if err != nil {
			ch <- res{nil, err}
			return
		}
		s, err := session.Handshake(ctx, session.Config{
			Conn: conn, Initiator: false, Authorize: grant,
			Local: session.Local{NodeID: ownerFP, Folders: []session.Folder{{ShareID: folderID}}},
		})
		ch <- res{s, err}
	}()
	rconn, err := replicaTr.Dial(ctx, ownerTr.LocalAddr().String(), ownerFP)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	rs, err := session.Handshake(ctx, session.Config{
		Conn: rconn, Initiator: true, Authorize: grant,
		Local: session.Local{NodeID: replicaFP, Folders: []session.Folder{{ShareID: folderID}}},
	})
	if err != nil {
		t.Fatalf("replica handshake: %v", err)
	}
	or := <-ch
	if or.err != nil {
		t.Fatalf("owner handshake: %v", or.err)
	}

	wireEngines(t, ctx, or.s, rs, owner, replica)
	waitConverged(t, owner, replica)
	assertTreesEqual(t, owner.root, replica.root)
	assertLeafSetsEqual(t, owner, replica)
}
