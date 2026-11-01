package shipper

import (
	"context"
	"time"

	"github.com/m-pedro/gologship/internal/model"
)

// Batcher groups individual LogEvents into BatchPayloads triggered by
// either reaching maxBatchSize or flushInterval elapsing on a time.Ticker.
type Batcher struct {
	maxBatchSize  int
	flushInterval time.Duration
}

func NewBatcher(maxBatchSize int, flushInterval time.Duration) *Batcher {
	if maxBatchSize <= 0 {
		maxBatchSize = 100
	}
	if flushInterval <= 0 {
		flushInterval = 500 * time.Millisecond
	}
	return &Batcher{
		maxBatchSize:  maxBatchSize,
		flushInterval: flushInterval,
	}
}

// Run reads events from `in` and emits BatchPayloads to `out`.
// Flushes any remaining buffered events before closing `out` on shutdown.
func (b *Batcher) Run(ctx context.Context, in <-chan model.LogEvent, out chan<- model.BatchPayload) {
	defer close(out)

	buffer := make([]model.LogEvent, 0, b.maxBatchSize)
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		batch := make([]model.LogEvent, len(buffer))
		copy(batch, buffer)
		buffer = buffer[:0]

		payload := model.BatchPayload{
			SentAt: time.Now().UTC(),
			Count:  len(batch),
			Events: batch,
		}
		select {
		case out <- payload:
		case <-ctx.Done():
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case ev, ok := <-in:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, ev)
			if len(buffer) >= b.maxBatchSize {
				flush()
				ticker.Reset(b.flushInterval)
			}
		case <-ticker.C:
			flush()
		}
	}
}
