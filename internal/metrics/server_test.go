package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsAndHealthzEndpoints(t *testing.T) {
	handler := NewHandler(func() Snapshot {
		return Snapshot{
			ParsedLines:   1250,
			ParseFailures: 3,
			AlertsFired:   2,
			ShippedEvents: 1200,
			FailedBatches: 0,
			RetryTotal:    4,
		}
	})

	srv := httptest.NewServer(handler)
	defer srv.Close()

	// 1. Verify /healthz
	respH, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer respH.Body.Close()
	if respH.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /healthz, got %d", respH.StatusCode)
	}

	// 2. Verify /metrics Prometheus exposition format
	respM, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer respM.Body.Close()
	bodyBytes, _ := io.ReadAll(respM.Body)
	body := string(bodyBytes)

	expectedLines := []string{
		"gologship_parsed_lines_total 1250",
		"gologship_parse_failures_total 3",
		"gologship_alerts_fired_total 2",
		"gologship_shipped_events_total 1200",
		"gologship_http_retries_total 4",
	}
	for _, want := range expectedLines {
		if !strings.Contains(body, want) {
			t.Errorf("expected /metrics output to contain %q, got:\n%s", want, body)
		}
	}
}
