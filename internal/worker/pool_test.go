package worker

import (
	"context"
	"testing"

	"github.com/m-pedro/gologship/internal/model"
)

func TestPool_ConcurrentFanOutAndRaceSafety(t *testing.T) {
	ctx := context.Background()
	in := make(chan string, 200)
	out := make(chan model.LogEvent, 200)

	pool := NewPool(4)

	for i := 0; i < 100; i++ {
		in <- `192.168.1.1 - - [25/Oct/2026:12:00:00 +0000] "GET /api/v1/items HTTP/1.1" 200 512 4.20`
	}
	for i := 0; i < 15; i++ {
		in <- `corrupted_log_line_without_brackets`
	}
	close(in)

	pool.Run(ctx, in, out)

	var events []model.LogEvent
	for ev := range out {
		events = append(events, ev)
	}

	if len(events) != 100 {
		t.Fatalf("expected 100 parsed events, got %d", len(events))
	}
	if pool.ParsedCount() != 100 {
		t.Fatalf("expected atomic ParsedCount == 100, got %d", pool.ParsedCount())
	}
	if pool.ParseFailures() != 15 {
		t.Fatalf("expected atomic ParseFailures == 15, got %d", pool.ParseFailures())
	}
}
