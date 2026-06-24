package discovery

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
	"github.com/GhentiLabs/Trove/pkg/identity"
	"github.com/coder/websocket"
)

// fakeSignalServer starts an mTLS WS server that reads the opening Hello and then
// hands the connection to serve. It returns a trove:// URL pinning its fingerprint.
func fakeSignalServer(t *testing.T, serve func(ctx context.Context, c *websocket.Conn)) string {
	t.Helper()
	srvCert, srvID := newIdentity(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/signal", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		ctx := r.Context()
		if _, _, err := c.Read(ctx); err != nil { // opening Hello
			return
		}
		serve(ctx, c)
	})
	s := httptest.NewUnstartedServer(mux)
	s.TLS = identity.ServerTLSConfig(srvCert)
	s.StartTLS()
	t.Cleanup(s.Close)
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return "trove://" + u.Host + "?id=" + srvID
}

func readSignal(t *testing.T, ctx context.Context, c *websocket.Conn) disco.SignalMessage {
	t.Helper()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("server read: %v", err)
	}
	var msg disco.SignalMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("server unmarshal: %v", err)
	}
	return msg
}

func writeSignal(ctx context.Context, c *websocket.Conn, typ disco.SignalType, payload any) error {
	msg, err := disco.NewSignalMessage(typ, payload)
	if err != nil {
		return err
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, data)
}

func dialSignaler(t *testing.T, troveURL string, cert tls.Certificate) *Signaler {
	t.Helper()
	c := newClient(t, troveURL, cert)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sig, err := c.Signal(ctx)
	if err != nil {
		t.Fatalf("Signal: %v", err)
	}
	t.Cleanup(func() { _ = sig.Close() })
	return sig
}

func TestSignalConnectReturnsPeerCandidates(t *testing.T) {
	cliCert, _ := newIdentity(t)
	target := "dddddddddddddddddddddddddddddddddddddddddddddddddddd"
	url := fakeSignalServer(t, func(ctx context.Context, c *websocket.Conn) {
		msg := readSignal(t, ctx, c)
		if msg.Type != disco.SignalConnectRequest {
			return
		}
		var req disco.ConnectRequest
		_ = msg.Decode(&req)
		_ = writeSignal(ctx, c, disco.SignalPeerCandidates, disco.PeerCandidates{
			FromNodeID:    req.TargetNodeID,
			Candidates:    []disco.Address{{IP: "198.51.100.9", Port: 22000, Type: disco.AddressPublic}},
			PunchAtMillis: 1234567,
		})
		<-ctx.Done()
	})

	sig := dialSignaler(t, url, cliCert)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pc, err := sig.Connect(ctx, target, []disco.Address{{IP: "192.0.2.5", Port: 22000, Type: disco.AddressLAN}})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if pc.FromNodeID != target || pc.PunchAtMillis != 1234567 || len(pc.Candidates) != 1 {
		t.Fatalf("PeerCandidates = %+v", pc)
	}
}

func TestSignalConnectTargetUnavailable(t *testing.T) {
	cliCert, _ := newIdentity(t)
	target := "dddddddddddddddddddddddddddddddddddddddddddddddddddd"
	url := fakeSignalServer(t, func(ctx context.Context, c *websocket.Conn) {
		msg := readSignal(t, ctx, c)
		var req disco.ConnectRequest
		_ = msg.Decode(&req)
		_ = writeSignal(ctx, c, disco.SignalTargetUnavailable, disco.TargetUnavailable{TargetNodeID: req.TargetNodeID})
		<-ctx.Done()
	})

	sig := dialSignaler(t, url, cliCert)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := sig.Connect(ctx, target, nil); !errors.Is(err, ErrTargetUnavailable) {
		t.Fatalf("Connect to offline target err = %v, want ErrTargetUnavailable", err)
	}
}

func TestSignalReceivesIncoming(t *testing.T) {
	cliCert, _ := newIdentity(t)
	from := "dddddddddddddddddddddddddddddddddddddddddddddddddddd"
	url := fakeSignalServer(t, func(ctx context.Context, c *websocket.Conn) {
		_ = writeSignal(ctx, c, disco.SignalIncomingRequest, disco.IncomingRequest{
			FromNodeID:    from,
			Candidates:    []disco.Address{{IP: "203.0.113.7", Port: 22000, Type: disco.AddressPublic}},
			PunchAtMillis: 99,
		})
		<-ctx.Done()
	})

	sig := dialSignaler(t, url, cliCert)
	select {
	case ir := <-sig.Incoming():
		if ir.FromNodeID != from || ir.PunchAtMillis != 99 {
			t.Fatalf("IncomingRequest = %+v", ir)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive IncomingRequest")
	}
}
