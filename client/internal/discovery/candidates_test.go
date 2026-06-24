package discovery

import (
	"testing"

	disco "github.com/GhentiLabs/Trove/pkg/discovery"
)

func TestLocalCandidates(t *testing.T) {
	cands, err := LocalCandidates(22000)
	if err != nil {
		t.Fatalf("LocalCandidates: %v", err)
	}
	for _, a := range cands {
		if a.Port != 22000 {
			t.Fatalf("candidate %s has port %d, want 22000", a, a.Port)
		}
		if a.Type != disco.AddressLAN && a.Type != disco.AddressPublic {
			t.Fatalf("candidate %s has unexpected type %q", a, a.Type)
		}
		if err := a.Validate(); err != nil {
			t.Fatalf("candidate %s is not routable: %v", a, err)
		}
	}
}
