package detector

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/m-pedro/gologship/internal/model"
)

// Config defines the thresholds for real-time anomaly detection.
type Config struct {
	WindowSize         int
	MinSamples         int
	Max5xxErrorRate    float64
	MaxP99LatencyMs    float64
	AlertCooldown      time.Duration
	Clock              func() time.Time
}

// Detector inspects incoming LogEvents, updates the sliding window,
// and emits AnomalyAlerts when thresholds are breached.
type Detector struct {
	cfg        Config
	window     *RingWindow
	lastAlert  time.Time
	alertCount atomic.Uint64
}

func NewDetector(cfg Config) *Detector {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = 200
	}
	if cfg.MinSamples <= 0 {
		cfg.MinSamples = 25
	}
	if cfg.Max5xxErrorRate <= 0 {
		cfg.Max5xxErrorRate = 0.10
	}
	if cfg.MaxP99LatencyMs <= 0 {
		cfg.MaxP99LatencyMs = 250.0
	}
	if cfg.AlertCooldown <= 0 {
		cfg.AlertCooldown = 5 * time.Second
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Detector{
		cfg:    cfg,
		window: NewRingWindow(cfg.WindowSize),
	}
}

// Evaluate records a single event and returns a non-nil AnomalyAlert if thresholds are breached.
func (d *Detector) Evaluate(ev model.LogEvent) *model.AnomalyAlert {
	d.window.Add(ev)
	total, errRate, p99 := d.window.Snapshot()
	if total < d.cfg.MinSamples {
		return nil
	}

	now := d.cfg.Clock()
	if !d.lastAlert.IsZero() && now.Sub(d.lastAlert) < d.cfg.AlertCooldown {
		return nil
	}

	var reason string
	if errRate >= d.cfg.Max5xxErrorRate {
		reason = fmt.Sprintf("5xx error rate %.1f%% exceeded threshold %.1f%%", errRate*100, d.cfg.Max5xxErrorRate*100)
	} else if p99 >= d.cfg.MaxP99LatencyMs {
		reason = fmt.Sprintf("p99 latency %.2fms exceeded threshold %.2fms", p99, d.cfg.MaxP99LatencyMs)
	} else {
		return nil
	}

	d.lastAlert = now
	d.alertCount.Add(1)
	return &model.AnomalyAlert{
		TriggeredAt: now,
		Reason:      reason,
		WindowTotal: total,
		ErrorRate:   errRate,
		P99Latency:  p99,
	}
}

// Run consumes events from `in`, forwards every event to `out` for shipping,
// and emits any triggered alerts to `alerts` (non-blocking if `alerts` is full).
func (d *Detector) Run(
	ctx context.Context,
	in <-chan model.LogEvent,
	out chan<- model.LogEvent,
	alerts chan<- model.AnomalyAlert,
) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-in:
			if !ok {
				return
			}
			if alert := d.Evaluate(ev); alert != nil && alerts != nil {
				select {
				case alerts <- *alert:
				default:
				}
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (d *Detector) AlertCount() uint64 {
	return d.alertCount.Load()
}
