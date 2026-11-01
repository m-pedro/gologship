package detector

import (
	"testing"
	"time"

	"github.com/m-pedro/gologship/internal/model"
)

func TestDetector_5xxSpikeAndCooldown(t *testing.T) {
	fakeNow := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

	det := NewDetector(Config{
		WindowSize:      50,
		MinSamples:      10,
		Max5xxErrorRate: 0.20,
		MaxP99LatencyMs: 500.0,
		AlertCooldown:   10 * time.Second,
		Clock:           func() time.Time { return fakeNow },
	})

	// 1. Feed 8 healthy 200 OK events + 2 500 errors (20% error rate at sample #10)
	for i := 0; i < 8; i++ {
		if a := det.Evaluate(model.LogEvent{StatusCode: 200, LatencyMs: 12.0}); a != nil {
			t.Fatalf("unexpected alert on healthy traffic: %+v", a)
		}
	}
	_ = det.Evaluate(model.LogEvent{StatusCode: 500, LatencyMs: 40.0})
	alert := det.Evaluate(model.LogEvent{StatusCode: 503, LatencyMs: 45.0})
	if alert == nil {
		t.Fatal("expected 5xx spike alert at 20% error rate, got nil")
	}

	// 2. Immediate subsequent 500 error should be suppressed by 10s AlertCooldown
	fakeNow = fakeNow.Add(3 * time.Second)
	if suppressed := det.Evaluate(model.LogEvent{StatusCode: 500, LatencyMs: 50.0}); suppressed != nil {
		t.Fatalf("expected cooldown to suppress alert, got %+v", suppressed)
	}

	// 3. Advance clock past 10s cooldown -> alert should fire again
	fakeNow = fakeNow.Add(8 * time.Second)
	if second := det.Evaluate(model.LogEvent{StatusCode: 502, LatencyMs: 60.0}); second == nil {
		t.Fatal("expected alert to fire after cooldown elapsed")
	}

	if det.AlertCount() != 2 {
		t.Fatalf("expected AlertCount() == 2, got %d", det.AlertCount())
	}
}

func TestRingWindow_EvictsOldestEntries(t *testing.T) {
	w := NewRingWindow(10)
	// Fill ring buffer with 10 500-status events (100% error rate)
	for i := 0; i < 10; i++ {
		w.Add(model.LogEvent{StatusCode: 500, LatencyMs: 100.0})
	}
	_, errRate, _ := w.Snapshot()
	if errRate != 1.0 {
		t.Fatalf("expected 1.0 error rate, got %f", errRate)
	}

	// Overwrite all 10 slots with 200 OK events -> error rate must drop back to 0.0
	for i := 0; i < 10; i++ {
		w.Add(model.LogEvent{StatusCode: 200, LatencyMs: 5.0})
	}
	count, errRateAfter, p99After := w.Snapshot()
	if count != 10 || errRateAfter != 0.0 || p99After != 5.0 {
		t.Fatalf("expected ring buffer eviction to reset stats; got count=%d errRate=%f p99=%f", count, errRateAfter, p99After)
	}
}
