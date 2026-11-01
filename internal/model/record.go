package model

import "time"

// LogEvent represents a single parsed HTTP access log entry.
type LogEvent struct {
	IP         string  `json:"ip"`
	Timestamp  string  `json:"timestamp"`
	Method     string  `json:"method"`
	Path       string  `json:"path"`
	StatusCode int     `json:"status_code"`
	BytesSent  uint64  `json:"bytes_sent"`
	LatencyMs  float64 `json:"latency_ms"`
}

// AnomalyAlert is emitted when the sliding window detects an error or latency spike.
type AnomalyAlert struct {
	TriggeredAt time.Time `json:"triggered_at"`
	Reason      string    `json:"reason"`
	WindowTotal int       `json:"window_total"`
	ErrorRate   float64   `json:"error_rate"`
	P99Latency  float64   `json:"p99_latency_ms"`
}

// BatchPayload is the JSON payload shipped to the downstream HTTP ingestion sink.
type BatchPayload struct {
	SentAt time.Time  `json:"sent_at"`
	Count  int        `json:"count"`
	Events []LogEvent `json:"events"`
}
