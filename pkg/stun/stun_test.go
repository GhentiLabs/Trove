package stun

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"
)

func TestRequestRoundTrip(t *testing.T) {
	id, err := NewTxID()
	if err != nil {
		t.Fatalf("NewTxID: %v", err)
	}
	req := AppendBindingRequest(nil, id)
	if !Looks(req) {
		t.Fatal("Looks(request) = false")
	}
	got, ok := ParseRequest(req)
	if !ok {
		t.Fatal("ParseRequest returned ok=false")
	}
	if got != id {
		t.Fatalf("txid round-trip: got %x want %x", got, id)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	for _, addr := range []string{"192.0.2.1:32853", "203.0.113.9:65535", "[2001:db8::1]:443", "[fe80::1]:1"} {
		t.Run(addr, func(t *testing.T) {
			ap := netip.MustParseAddrPort(addr)
			id, _ := NewTxID()
			resp := AppendBindingResponse(nil, id, ap)
			if !Looks(resp) {
				t.Fatal("Looks(response) = false")
			}
			gotID, gotAP, ok := ParseResponse(resp)
			if !ok {
				t.Fatal("ParseResponse ok=false")
			}
			if gotID != id {
				t.Fatalf("txid: got %x want %x", gotID, id)
			}
			if gotAP != ap {
				t.Fatalf("addr: got %v want %v", gotAP, ap)
			}
		})
	}
}

// TestRFC5769Vector checks the XOR-MAPPED-ADDRESS encoding against the IPv4 sample
// response in RFC 5769 §2.2: 192.0.2.1:32853 XORs to port a147, address e112a643.
func TestRFC5769Vector(t *testing.T) {
	id := TxID{0xb7, 0xe7, 0xa7, 0x01, 0xbc, 0x34, 0xd6, 0x86, 0xfa, 0x87, 0xdf, 0xae}
	resp := AppendBindingResponse(nil, id, netip.MustParseAddrPort("192.0.2.1:32853"))
	// header(20) + attr header(4) + value: reserved/family/xport/xaddr.
	val := resp[headerSize+4:]
	want := []byte{0x00, 0x01, 0xa1, 0x47, 0xe1, 0x12, 0xa6, 0x43}
	if !bytes.Equal(val, want) {
		t.Fatalf("XOR-MAPPED-ADDRESS = % x, want % x", val, want)
	}
}

func TestLooksRejectsNonSTUN(t *testing.T) {
	cases := [][]byte{
		nil,
		{0x01},
		bytes.Repeat([]byte{0xff}, 20), // high bits set, no cookie
		append([]byte{0x00, 0x01, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef}, bytes.Repeat([]byte{0}, 12)...), // wrong cookie
	}
	for i, b := range cases {
		if Looks(b) {
			t.Fatalf("case %d: Looks = true, want false", i)
		}
	}
}

// TestParseResponseUnalignedTrailingAttr crafts a response whose final attribute
// has a non-4-aligned length running to the end of the message — the case that
// previously overran the attribute slice. It must reject cleanly, never panic.
func TestParseResponseUnalignedTrailingAttr(t *testing.T) {
	id, _ := NewTxID()
	var b []byte
	b = binary.BigEndian.AppendUint16(b, typeBindingResp)
	b = binary.BigEndian.AppendUint16(b, 5) // message length = one 5-byte attribute
	b = binary.BigEndian.AppendUint32(b, MagicCookie)
	b = append(b, id[:]...)
	b = binary.BigEndian.AppendUint16(b, 0x8022) // SOFTWARE (not XOR-MAPPED-ADDRESS)
	b = binary.BigEndian.AppendUint16(b, 1)      // length 1 — not 4-aligned, ends the msg
	b = append(b, 0x00)
	if _, _, ok := ParseResponse(b); ok {
		t.Fatal("ParseResponse accepted a malformed response")
	}
}

// FuzzParseResponse asserts the response decoder never panics on arbitrary input.
func FuzzParseResponse(f *testing.F) {
	id, _ := NewTxID()
	f.Add(AppendBindingResponse(nil, id, netip.MustParseAddrPort("192.0.2.1:1")))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = ParseResponse(data)
	})
}

func TestParseResponseTruncated(t *testing.T) {
	id, _ := NewTxID()
	resp := AppendBindingResponse(nil, id, netip.MustParseAddrPort("192.0.2.1:1"))
	for n := range len(resp) {
		if _, _, ok := ParseResponse(resp[:n]); ok {
			t.Fatalf("ParseResponse(truncated to %d) = ok, want false", n)
		}
	}
}
