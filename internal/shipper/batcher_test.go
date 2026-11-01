package shipper

import (
	"context"
	"testing"
	"time"

	"github.com/m-pedro/gologship/internal/model"
)

func TestBatcher_FlushesOnSizeAndTickerAndClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	in := make(chan model.LogEvent, 16)
	out := make(chan model.BatchPayload, 16)

	batcher := NewBatcher(3, 40*time.Millisecond)
	go batcher.Run(ctx, in, out)

	// 1. Send 3 events -> should flush immediately on maxBatchSize (3)
	for i := 0; i < 3; i++ {
		in <- model.LogEvent{IP: "10.0.0.1", StatusCode: 200}
	}
	b1 := <-out
	if b1.Count != 3 {
		t.Fatalf("expected size-triggered batch of 3, got %d", b1.Count)
	}

	// 2. Send 1 event and wait for the 40ms ticker to flush it
	in <- model.LogEvent{IP: "10.0.0.2", StatusCode: 201}
	b2 := <-out
	if b2.Count != 1 {
		t.Fatalf("expected ticker-triggered batch of 1, got %d", b2.Count)
	}

	// 3. Send 2 events and close `in` -> should flush remaining 2 on channel close
	in <- model.LogEvent{IP: "10.0.0.3", StatusCode: 200}
	in <- model.LogEvent{IP: "10.0.0.4", StatusCode: 200}
	close(in)

	b3 := <-out
	if b3.Count != 2 {
		t.Fatalf("expected final drain batch of 2, got %d", b3.Count)
	}
}
