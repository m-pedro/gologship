package internal

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m-pedro/gologship/internal/detector"
	"github.com/m-pedro/gologship/internal/model"
	"github.com/m-pedro/gologship/internal/shipper"
	"github.com/m-pedro/gologship/internal/tailer"
	"github.com/m-pedro/gologship/internal/worker"
)

func TestEndToEndPipeline_WithRaceDetector(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "live.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create log file: %v", err)
	}
	defer f.Close()

	// Write 40 valid lines (including 12 500-errors to trigger anomaly alert) + 5 malformed lines
	for i := 0; i < 28; i++ {
		fmt.Fprintf(f, "10.0.0.%d - - [25/Oct/2026:12:00:00 +0000] \"GET /api/v1/ok HTTP/1.1\" 200 512 10.5\n", i%10)
	}
	for i := 0; i < 12; i++ {
		fmt.Fprintf(f, "10.0.0.99 - - [25/Oct/2026:12:00:01 +0000] \"POST /api/v1/pay HTTP/1.1\" 503 128 450.0\n")
	}
	for i := 0; i < 5; i++ {
		fmt.Fprintf(f, "malformed_garbage_line_%d\n", i)
	}
	_ = f.Sync()

	var receivedBatches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBatches.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tailCtx, cancelTail := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancelTail()

	tl := tailer.New(tailer.Config{FilePath: logPath, PollInterval: 10 * time.Millisecond})
	pool := worker.NewPool(4)
	det := detector.NewDetector(detector.Config{
		WindowSize:      50,
		MinSamples:      20,
		Max5xxErrorRate: 0.20,
		MaxP99LatencyMs: 300.0,
		AlertCooldown:   1 * time.Second,
	})
	batcher := shipper.NewBatcher(15, 30*time.Millisecond)
	client := shipper.NewClient(shipper.ClientConfig{
		Endpoint:  srv.URL,
		BaseDelay: 5 * time.Millisecond,
	})

	rawCh := make(chan string, 128)
	parsedCh := make(chan model.LogEvent, 128)
	checkedCh := make(chan model.LogEvent, 128)
	alertsCh := make(chan model.AnomalyAlert, 16)
	batchesCh := make(chan model.BatchPayload, 16)

	go func() {
		_ = tl.Stream(tailCtx, rawCh)
		close(rawCh)
	}()
	go pool.Run(context.Background(), rawCh, parsedCh)
	go func() {
		det.Run(context.Background(), parsedCh, checkedCh, alertsCh)
		close(alertsCh)
	}()
	go batcher.Run(context.Background(), checkedCh, batchesCh)

	for batch := range batchesCh {
		if err := client.SendBatch(context.Background(), batch); err != nil {
			t.Fatalf("SendBatch failed: %v", err)
		}
	}

	if pool.ParsedCount() != 40 {
		t.Fatalf("expected 40 valid parsed events, got %d", pool.ParsedCount())
	}
	if pool.ParseFailures() != 5 {
		t.Fatalf("expected 5 parse failures, got %d", pool.ParseFailures())
	}
	if det.AlertCount() == 0 {
		t.Fatal("expected detector to fire at least 1 anomaly alert on 503 spike")
	}
	if client.ShippedEvents() != 40 {
		t.Fatalf("expected 40 shipped events, got %d", client.ShippedEvents())
	}
	if receivedBatches.Load() < 2 {
		t.Fatalf("expected at least 2 HTTP batches, got %d", receivedBatches.Load())
	}
}
