package discovery

import "encoding/json"

// SignalType discriminates WebSocket signaling messages.
type SignalType string

const (
	SignalHello             SignalType = "hello"
	SignalConnectRequest    SignalType = "connect_request"
	SignalIncomingRequest   SignalType = "incoming_request"
	SignalPeerCandidates    SignalType = "peer_candidates"
	SignalTargetUnavailable SignalType = "target_unavailable"
	SignalError             SignalType = "error"
)

// SignalMessage is the framing for every WebSocket message. Payload holds the
// type-specific body.
type SignalMessage struct {
	Type    SignalType      `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Decode unmarshals the message payload into v.
func (m SignalMessage) Decode(v any) error {
	return json.Unmarshal(m.Payload, v)
}

// NewSignalMessage frames a typed payload for sending.
func NewSignalMessage(typ SignalType, payload any) (SignalMessage, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return SignalMessage{}, err
	}
	return SignalMessage{Type: typ, Payload: raw}, nil
}

// Hello opens a signaling session. The connection is already mTLS-authenticated,
// so it carries no fields.
type Hello struct{}

// ConnectRequest asks the server to broker a hole-punch with a target node.
type ConnectRequest struct {
	TargetNodeID string    `json:"target_node_id"`
	MyCandidates []Address `json:"my_candidates"`
}

// IncomingRequest is delivered to the target of a ConnectRequest.
type IncomingRequest struct {
	FromNodeID    string    `json:"from_node_id"`
	Candidates    []Address `json:"candidates"`
	PunchAtMillis int64     `json:"punch_at_millis"`
}

// PeerCandidates is the reply to a requester whose target was reachable.
type PeerCandidates struct {
	FromNodeID    string    `json:"from_node_id"`
	Candidates    []Address `json:"candidates"`
	PunchAtMillis int64     `json:"punch_at_millis"`
}

// TargetUnavailable tells a requester the target has no live connection.
type TargetUnavailable struct {
	TargetNodeID string `json:"target_node_id"`
}

// SignalErrorPayload reports a signaling-level error to the client.
type SignalErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
