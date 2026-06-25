// Package stun implements the discovery server's UDP STUN responder. A peer sends
// a Binding request from its QUIC socket and the responder echoes the observed
// source as an XOR-MAPPED-ADDRESS, so the peer learns the external ip:port that
// socket is mapped to — the reflexive candidate other peers punch toward.
//
// The responder is stateless and answers only Binding requests. A response is small
// and only modestly larger than the request (~32 vs ~20 bytes), so reflection toward
// a spoofed source has a low amplification factor; a global rate limit further bounds
// the reflected volume and the work an abuser can induce.
package stun

import (
	"errors"
	"log/slog"
	"net"
	"net/netip"

	"golang.org/x/time/rate"

	pkgstun "github.com/GhentiLabs/Trove/pkg/stun"
)

// maxRequestSize caps a read; Binding requests are tiny, so anything larger is not
// a request we answer.
const maxRequestSize = 512

// Server answers STUN Binding requests on a UDP socket.
type Server struct {
	conn *net.UDPConn
	log  *slog.Logger
	lim  *rate.Limiter
}

// Options configures New.
type Options struct {
	// Conn is the bound UDP socket the responder reads from and writes to.
	Conn *net.UDPConn
	// Logger receives responder events; nil discards them.
	Logger *slog.Logger
	// RatePerSec and Burst bound the total Binding responses emitted per second.
	RatePerSec float64
	Burst      int
}

// New builds a Server. Call Serve to run it.
func New(opts Options) *Server {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Server{
		conn: opts.Conn,
		log:  log,
		lim:  rate.NewLimiter(rate.Limit(opts.RatePerSec), opts.Burst),
	}
}

// Serve reads and answers Binding requests until the socket is closed. It returns
// nil on a clean close and the read error otherwise.
func (s *Server) Serve() error {
	buf := make([]byte, maxRequestSize)
	for {
		n, from, err := s.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		id, ok := pkgstun.ParseRequest(buf[:n])
		if !ok {
			continue
		}
		if !s.lim.Allow() {
			continue
		}
		from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
		if _, err := s.conn.WriteToUDPAddrPort(pkgstun.AppendBindingResponse(nil, id, from), from); err != nil {
			s.log.Debug("stun: write response failed", "to", from, "err", err)
		}
	}
}

// Close stops the responder by closing its socket.
func (s *Server) Close() error { return s.conn.Close() }
