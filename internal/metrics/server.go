package metrics

import (
	"fmt"
	"net/http"
)

// Snapshot holds point-in-time pipeline telemetry for Prometheus scraping.
type Snapshot struct {
	ParsedLines   uint64
	ParseFailures uint64
	AlertsFired   uint64
	ShippedEvents uint64
	FailedBatches uint64
	RetryTotal    uint64
}

// Provider is a callback returning the latest atomic counter values.
type Provider func() Snapshot

// NewHandler returns an http.Handler serving /metrics (Prometheus text format) and /healthz.
func NewHandler(provider Provider) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := provider()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintf(w, "# HELP gologship_parsed_lines_total Total HTTP log lines successfully parsed.\n")
		fmt.Fprintf(w, "# TYPE gologship_parsed_lines_total counter\n")
		fmt.Fprintf(w, "gologship_parsed_lines_total %d\n", s.ParsedLines)

		fmt.Fprintf(w, "# HELP gologship_parse_failures_total Total malformed log lines rejected.\n")
		fmt.Fprintf(w, "# TYPE gologship_parse_failures_total counter\n")
		fmt.Fprintf(w, "gologship_parse_failures_total %d\n", s.ParseFailures)

		fmt.Fprintf(w, "# HELP gologship_alerts_fired_total Total sliding-window anomaly alerts triggered.\n")
		fmt.Fprintf(w, "# TYPE gologship_alerts_fired_total counter\n")
		fmt.Fprintf(w, "gologship_alerts_fired_total %d\n", s.AlertsFired)

		fmt.Fprintf(w, "# HELP gologship_shipped_events_total Total log events shipped to HTTP sink.\n")
		fmt.Fprintf(w, "# TYPE gologship_shipped_events_total counter\n")
		fmt.Fprintf(w, "gologship_shipped_events_total %d\n", s.ShippedEvents)

		fmt.Fprintf(w, "# HELP gologship_failed_batches_total Total batches dropped after retry exhaustion.\n")
		fmt.Fprintf(w, "# TYPE gologship_failed_batches_total counter\n")
		fmt.Fprintf(w, "gologship_failed_batches_total %d\n", s.FailedBatches)

		fmt.Fprintf(w, "# HELP gologship_http_retries_total Total transient HTTP retry attempts.\n")
		fmt.Fprintf(w, "# TYPE gologship_http_retries_total counter\n")
		fmt.Fprintf(w, "gologship_http_retries_total %d\n", s.RetryTotal)
	})

	return mux
}
