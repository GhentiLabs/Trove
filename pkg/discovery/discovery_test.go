package discovery

import (
	"encoding/json"
	"testing"
)

func TestAddressValidate(t *testing.T) {
	valid := []Address{
		{IP: "192.168.1.5", Port: 9000, Type: AddressLAN},
		{IP: "2001:db8::1", Port: 1, Type: AddressPublic},
		{IP: "8.8.8.8", Port: 65535, Type: AddressSTUN},
	}
	for _, a := range valid {
		if err := a.Validate(); err != nil {
			t.Errorf("Validate(%v) = %v, want nil", a, err)
		}
	}
	invalid := []Address{
		{IP: "not-an-ip", Port: 80, Type: AddressLAN},
		{IP: "127.0.0.1", Port: 80, Type: AddressLAN},
		{IP: "1.2.3.4", Port: 0, Type: AddressLAN},
		{IP: "1.2.3.4", Port: 70000, Type: AddressLAN},
		{IP: "1.2.3.4", Port: 80, Type: "bogus"},
	}
	for _, a := range invalid {
		if err := a.Validate(); err == nil {
			t.Errorf("Validate(%v) = nil, want error", a)
		}
	}
}

func TestSignalMessageRoundTrip(t *testing.T) {
	in := ConnectRequest{TargetNodeID: "abc", MyCandidates: []Address{{IP: "1.1.1.1", Port: 9, Type: AddressPublic}}}
	msg, err := NewSignalMessage(SignalConnectRequest, in)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded SignalMessage
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Type != SignalConnectRequest {
		t.Fatalf("type = %q, want %q", decoded.Type, SignalConnectRequest)
	}
	var out ConnectRequest
	if err := decoded.Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.TargetNodeID != in.TargetNodeID || len(out.MyCandidates) != 1 {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}
