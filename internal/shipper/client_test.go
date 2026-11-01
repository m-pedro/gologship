package shipper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-pedro/gologship/internal/model"
)

func TestClient_RecoversFromTransient503Errors(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:   srv.URL,
		MaxRetries: 4,
		BaseDelay:  5 * time.Millisecond,
		MaxDelay:   25 * time.Millisecond,
	})

	batch := model.BatchPayload{
		SentAt: time.Now(),
		Count:  2,
		Events: []model.LogEvent{
			{IP: "10.0.0.1", StatusCode: 200},
			{IP: "10.0.0.2", StatusCode: 200},
		},
	}

	if err := client.SendBatch(context.Background(), batch); err != nil {
		t.Fatalf("expected retry recovery on 3rd attempt, got error: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 HTTP attempts (2 x 503 + 1 x 202), got %d", attempts.Load())
	}
	if client.ShippedEvents() != 2 || client.RetryTotal() != 2 || client.FailedBatches() != 0 {
		t.Fatalf("unexpected client metrics: shipped=%d retries=%d failed=%d",
			client.ShippedEvents(), client.RetryTotal(), client.FailedBatches())
	}
}

func TestClient_DoesNotRetryPermanent400Error(t *testing.T) {
	var attempts atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:   srv.URL,
		MaxRetries: 4,
		BaseDelay:  5 * time.Millisecond,
	})

	err := client.SendBatch(context.Background(), model.BatchPayload{Count: 1})
	if err == nil {
		t.Fatal("expected error on 400 Bad Request, got nil")
	}
	if attempts.Load() != 1 {
		t.Fatalf("expected 400 error to fail immediately without retry; got %d attempts", attempts.Load())
	}
	if client.FailedBatches() != 1 {
		t.Fatalf("expected FailedBatches == 1, got %d", client.FailedBatches())
	}
}
