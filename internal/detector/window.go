package detector

import (
	"math"
	"sort"
	"sync"

	"github.com/m-pedro/gologship/internal/model"
)

type sample struct {
	is5xx     bool
	latencyMs float64
}

// RingWindow maintains a fixed-capacity circular buffer of recent LogEvents
// so memory usage stays strictly O(capacity) regardless of stream duration.
type RingWindow struct {
	mu         sync.Mutex
	buf        []sample
	capacity   int
	head       int
	count      int
	error5xx   int
	scratchLat []float64
}

func NewRingWindow(capacity int) *RingWindow {
	if capacity <= 0 {
		capacity = 200
	}
	return &RingWindow{
		buf:        make([]sample, capacity),
		capacity:   capacity,
		scratchLat: make([]float64, 0, capacity),
	}
}

// Add inserts a new event into the ring buffer, evicting the oldest entry if full.
func (w *RingWindow) Add(ev model.LogEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count == w.capacity {
		oldest := w.buf[w.head]
		if oldest.is5xx {
			w.error5xx--
		}
	} else {
		w.count++
	}

	isErr := ev.StatusCode >= 500 && ev.StatusCode < 600
	if isErr {
		w.error5xx++
	}

	w.buf[w.head] = sample{
		is5xx:     isErr,
		latencyMs: ev.LatencyMs,
	}
	w.head = (w.head + 1) % w.capacity
}

// Snapshot returns (totalSamples, error5xxRate, p99LatencyMs) across the active window.
func (w *RingWindow) Snapshot() (int, float64, float64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count == 0 {
		return 0, 0.0, 0.0
	}

	errRate := float64(w.error5xx) / float64(w.count)

	w.scratchLat = w.scratchLat[:0]
	for i := 0; i < w.count; i++ {
		w.scratchLat = append(w.scratchLat, w.buf[i].latencyMs)
	}
	sort.Float64s(w.scratchLat)

	rank := 0.99 * float64(w.count-1)
	idx := int(math.Round(rank))
	if idx >= w.count {
		idx = w.count - 1
	}

	return w.count, errRate, w.scratchLat[idx]
}
