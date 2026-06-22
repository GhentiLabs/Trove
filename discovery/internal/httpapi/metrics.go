package httpapi

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the server's Prometheus collectors. Counters are labelled by
// endpoint and outcome only: per-source detail lives in the structured logs,
// where it does not risk unbounded label cardinality.
type Metrics struct {
	requests      *prometheus.CounterVec
	signalMatches *prometheus.CounterVec
	signalActive  prometheus.Gauge
	registrySize  prometheus.Gauge
}

// NewMetrics registers and returns the server metrics.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trove_discovery_requests_total",
			Help: "Requests by endpoint and outcome.",
		}, []string{"endpoint", "outcome"}),
		signalMatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trove_discovery_signaling_matches_total",
			Help: "Signaling connect_request routing results.",
		}, []string{"result"}),
		signalActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trove_discovery_signaling_active_connections",
			Help: "Currently live signaling WebSocket connections.",
		}),
		registrySize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trove_discovery_registry_entries",
			Help: "Current number of registry entries.",
		}),
	}
	reg.MustRegister(m.requests, m.signalMatches, m.signalActive, m.registrySize)
	return m
}

func (m *Metrics) request(endpoint, outcome string) {
	m.requests.WithLabelValues(endpoint, outcome).Inc()
}

// SignalMatch records a signaling routing outcome for the broker's metrics hook.
func (m *Metrics) SignalMatch(success bool) {
	result := "matched"
	if !success {
		result = "unavailable"
	}
	m.signalMatches.WithLabelValues(result).Inc()
}

// SignalActiveDelta adjusts the live-connection gauge.
func (m *Metrics) SignalActiveDelta(delta int) { m.signalActive.Add(float64(delta)) }

// SetRegistrySize publishes the current registry size.
func (m *Metrics) SetRegistrySize(n int) { m.registrySize.Set(float64(n)) }
