package discovery

import (
	"context"
	"net"
	"testing"
	"time"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

// fakeNAT is a nat.NAT stand-in: AddPortMapping optionally blocks until block is
// closed, so the Refresh timeout can be exercised without a real gateway.
type fakeNAT struct {
	block   chan struct{}
	addPort int
	addErr  error
	delErr  error
}

func (f *fakeNAT) Type() string                        { return "fake" }
func (f *fakeNAT) GetDeviceAddress() (net.IP, error)   { return nil, nil }
func (f *fakeNAT) GetExternalAddress() (net.IP, error) { return net.ParseIP("203.0.113.7"), nil }
func (f *fakeNAT) GetInternalAddress() (net.IP, error) { return nil, nil }
func (f *fakeNAT) DeletePortMapping(string, int) error { return f.delErr }
func (f *fakeNAT) AddPortMapping(_ string, _ int, _ string, _ time.Duration) (int, error) {
	if f.block != nil {
		<-f.block
	}
	return f.addPort, f.addErr
}

func TestPubliclyRoutable(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"203.0.113.7", true},
		{"8.8.8.8", true},
		{"100.64.0.1", false}, // CGNAT (RFC 6598), not covered by IsPrivate
		{"100.127.255.1", false},
		{"10.0.0.1", false},
		{"192.168.1.1", false},
		{"127.0.0.1", false},
		{"0.0.0.0", false},
	}
	for _, c := range cases {
		if got := publiclyRoutable(net.ParseIP(c.ip)); got != c.want {
			t.Errorf("publiclyRoutable(%s) = %v, want %v", c.ip, got, c.want)
		}
	}
}

// TestPortMapRefreshTimeout proves a hung gateway cannot stall Refresh past ctx.
func TestPortMapRefreshTimeout(t *testing.T) {
	pm := &PortMap{
		nat:      &fakeNAT{block: make(chan struct{})},
		internal: 22000,
		External: disco.Address{IP: "203.0.113.7", Port: 1000, Type: disco.AddressPublic},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := pm.Refresh(ctx); err == nil {
		t.Fatal("expected a timeout error from a blocked gateway")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Refresh blocked %v despite ctx timeout", d)
	}
	if pm.External.Port != 1000 {
		t.Fatalf("External.Port mutated on timeout: %d", pm.External.Port)
	}
}

func TestPortMapRefreshUpdatesPort(t *testing.T) {
	pm := &PortMap{
		nat:      &fakeNAT{addPort: 2000},
		internal: 22000,
		External: disco.Address{IP: "203.0.113.7", Port: 1000, Type: disco.AddressPublic},
	}
	if err := pm.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if pm.External.Port != 2000 {
		t.Fatalf("External.Port = %d, want 2000", pm.External.Port)
	}
}
