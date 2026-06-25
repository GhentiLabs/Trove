package node

import (
	"net"
	"testing"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("ParseCIDR(%q): %v", s, err)
	}
	return n
}

func TestDialable(t *testing.T) {
	local := []*net.IPNet{mustCIDR(t, "192.168.1.0/24")}
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{"public always dialable", "203.0.113.7", true},
		{"private inside our subnet", "192.168.1.50", true},
		{"private outside our subnets", "10.0.0.5", false},
		{"private same family other subnet", "192.168.2.50", false},
		{"invalid ip", "nope", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dialable(disco.Address{IP: tt.ip, Port: 22000, Type: disco.AddressPublic}, local)
			if got != tt.want {
				t.Fatalf("dialable(%q) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}

func TestDialableNoLocalSubnets(t *testing.T) {
	// With no local subnets, the SSRF guard fails closed on private addresses.
	if dialable(disco.Address{IP: "192.168.1.50", Port: 1}, nil) {
		t.Fatal("private address dialable with no local subnets")
	}
	if !dialable(disco.Address{IP: "203.0.113.7", Port: 1}, nil) {
		t.Fatal("public address not dialable")
	}
}
