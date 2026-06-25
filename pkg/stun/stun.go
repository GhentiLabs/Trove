// Package stun implements the minimal subset of STUN (RFC 5389) that Trove needs
// for reflexive-address discovery: a Binding request and a Binding success
// response carrying XOR-MAPPED-ADDRESS. A peer sends a Binding request from its
// QUIC UDP socket to the discovery server's STUN responder and learns the external
// ip:port that socket is mapped to — the candidate other peers punch toward.
//
// There is no MESSAGE-INTEGRITY/FINGERPRINT: the reflexive address is not a
// secret (the QUIC session is still mTLS-pinned), so the extra round of shared
// secrets buys nothing here. Messages are wire-compatible with standard STUN, so
// a generic STUN server also works as a fallback responder.
package stun

import (
	"crypto/rand"
	"encoding/binary"
	"net/netip"
)

// MagicCookie is the fixed value at bytes 4..8 of every RFC 5389 message; it both
// identifies STUN traffic (for demultiplexing on a shared socket) and keys the
// XOR-MAPPED-ADDRESS obfuscation.
const MagicCookie uint32 = 0x2112A442

const (
	headerSize       = 20
	typeBindingReq   = 0x0001
	typeBindingResp  = 0x0101
	attrXORMappedAdr = 0x0020
)

// TxID is the 96-bit transaction id correlating a response to its request.
type TxID [12]byte

// NewTxID returns a random transaction id.
func NewTxID() (TxID, error) {
	var id TxID
	_, err := rand.Read(id[:])
	return id, err
}

// Looks reports whether b plausibly is a STUN message: the two most significant
// bits of the first byte are zero and the magic cookie is present. This is the
// RFC 5389 §6 demultiplexing check used to separate STUN from QUIC on one socket.
func Looks(b []byte) bool {
	return len(b) >= headerSize && b[0]&0xC0 == 0 &&
		binary.BigEndian.Uint32(b[4:8]) == MagicCookie
}

// AppendBindingRequest appends a Binding request with the given transaction id.
func AppendBindingRequest(dst []byte, id TxID) []byte {
	return appendHeader(dst, typeBindingReq, id, 0)
}

// ParseRequest reports whether b is a Binding request and returns its id.
func ParseRequest(b []byte) (TxID, bool) {
	var id TxID
	if !Looks(b) || binary.BigEndian.Uint16(b[0:2]) != typeBindingReq {
		return id, false
	}
	copy(id[:], b[8:20])
	return id, true
}

// AppendBindingResponse appends a Binding success response echoing addr as an
// XOR-MAPPED-ADDRESS attribute. addr must be valid (IPv4 or IPv6).
func AppendBindingResponse(dst []byte, id TxID, addr netip.AddrPort) []byte {
	ip := addr.Addr()
	attrLen := 8
	if ip.Is6() && !ip.Is4In6() {
		attrLen = 20
	}
	dst = appendHeader(dst, typeBindingResp, id, uint16(4+attrLen))
	dst = binary.BigEndian.AppendUint16(dst, attrXORMappedAdr)
	dst = binary.BigEndian.AppendUint16(dst, uint16(attrLen))
	return appendXORAddr(dst, id, addr)
}

// ParseResponse reports whether b is a Binding success response and returns its
// transaction id and the XOR-MAPPED-ADDRESS it carries.
func ParseResponse(b []byte) (TxID, netip.AddrPort, bool) {
	var id TxID
	if !Looks(b) || binary.BigEndian.Uint16(b[0:2]) != typeBindingResp {
		return id, netip.AddrPort{}, false
	}
	copy(id[:], b[8:20])
	msgLen := int(binary.BigEndian.Uint16(b[2:4]))
	if headerSize+msgLen > len(b) {
		return id, netip.AddrPort{}, false
	}
	attrs := b[headerSize : headerSize+msgLen]
	for len(attrs) >= 4 {
		atyp := binary.BigEndian.Uint16(attrs[0:2])
		alen := int(binary.BigEndian.Uint16(attrs[2:4]))
		if 4+alen > len(attrs) {
			break
		}
		val := attrs[4 : 4+alen]
		if atyp == attrXORMappedAdr {
			if ap, ok := parseXORAddr(id, val); ok {
				return id, ap, true
			}
			return id, netip.AddrPort{}, false
		}
		adv := 4 + align4(alen)
		if adv > len(attrs) {
			break
		}
		attrs = attrs[adv:]
	}
	return id, netip.AddrPort{}, false
}

func appendHeader(dst []byte, msgType uint16, id TxID, msgLen uint16) []byte {
	dst = binary.BigEndian.AppendUint16(dst, msgType)
	dst = binary.BigEndian.AppendUint16(dst, msgLen)
	dst = binary.BigEndian.AppendUint32(dst, MagicCookie)
	return append(dst, id[:]...)
}

func appendXORAddr(dst []byte, id TxID, addr netip.AddrPort) []byte {
	ip := addr.Addr()
	dst = append(dst, 0) // reserved
	port := addr.Port() ^ uint16(MagicCookie>>16)
	if ip.Is4() || ip.Is4In6() {
		dst = append(dst, 0x01)
		dst = binary.BigEndian.AppendUint16(dst, port)
		v4 := ip.As4()
		var cookie [4]byte
		binary.BigEndian.PutUint32(cookie[:], MagicCookie)
		for i := range v4 {
			v4[i] ^= cookie[i]
		}
		return append(dst, v4[:]...)
	}
	dst = append(dst, 0x02)
	dst = binary.BigEndian.AppendUint16(dst, port)
	v6 := ip.As16()
	mask := xorMask(id)
	for i := range v6 {
		v6[i] ^= mask[i]
	}
	return append(dst, v6[:]...)
}

func parseXORAddr(id TxID, val []byte) (netip.AddrPort, bool) {
	if len(val) < 8 {
		return netip.AddrPort{}, false
	}
	port := binary.BigEndian.Uint16(val[2:4]) ^ uint16(MagicCookie>>16)
	switch val[1] {
	case 0x01:
		var v4 [4]byte
		copy(v4[:], val[4:8])
		var cookie [4]byte
		binary.BigEndian.PutUint32(cookie[:], MagicCookie)
		for i := range v4 {
			v4[i] ^= cookie[i]
		}
		return netip.AddrPortFrom(netip.AddrFrom4(v4), port), true
	case 0x02:
		if len(val) < 20 {
			return netip.AddrPort{}, false
		}
		var v6 [16]byte
		copy(v6[:], val[4:20])
		mask := xorMask(id)
		for i := range v6 {
			v6[i] ^= mask[i]
		}
		return netip.AddrPortFrom(netip.AddrFrom16(v6), port), true
	default:
		return netip.AddrPort{}, false
	}
}

func xorMask(id TxID) [16]byte {
	var mask [16]byte
	binary.BigEndian.PutUint32(mask[0:4], MagicCookie)
	copy(mask[4:], id[:])
	return mask
}

func align4(n int) int { return (n + 3) &^ 3 }
