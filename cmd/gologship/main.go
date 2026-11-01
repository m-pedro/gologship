package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/m-pedro/gologship/internal/detector"
	"github.com/m-pedro/gologship/internal/metrics"
	"github.com/m-pedro/gologship/internal/model"
	"github.com/m-pedro/gologship/internal/shipper"
	"github.com/m-pedro/gologship/internal/tailer"
	"github.com/m-pedro/gologship/internal/worker"
)

func main() {
	filePath := flag.String("file", "data/live_access.log", "Path to HTTP access log file to tail")
	workers := flag.Int("workers", 4, "Number of concurrent parser worker goroutines")
	batchSize := flag.Int("batch-size", 100, "Max events per HTTP micro-batch")
	flushInterval := flag.Duration("flush-interval", 500*time.Millisecond, "Max time before flushing partial batch")
	sinkURL := flag.String("sink-url", "", "Downstream HTTP POST ingestion URL (empty = dry-run mode)")
	metricsAddr := flag.String("metrics-addr", ":9090", "Listen address for Prometheus /metrics and /healthz")
	windowSize := flag.Int("window-size", 200, "Sliding window sample capacity for anomaly detection")
	maxErrRate := flag.Float64("max-error-rate", 0.15, "5xx error rate threshold (0.15 = 15%)")
	maxP99Ms := flag.Float64("max-p99-ms", 300.0, "p99 latency threshold in milliseconds")
	runFor := flag.Duration("run-for", 0, "Optional duration before automatic clean shutdown (0 = run until Ctrl+C)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *runFor)
		defer cancel()
	}

	tl := tailer.New(tailer.Config{
		FilePath:     *filePath,
		PollInterval: 25 * time.Millisecond,
		StartAtEnd:   false,
	})
	pool := worker.NewPool(*workers)
	det := detector.NewDetector(detector.Config{
		WindowSize:      *windowSize,
		MinSamples:      20,
		Max5xxErrorRate: *maxErrRate,
		MaxP99LatencyMs: *maxP99Ms,
		AlertCooldown:   2 * time.Second,
	})
	batcher := shipper.NewBatcher(*batchSize, *flushInterval)
	client := shipper.NewClient(shipper.ClientConfig{
		Endpoint:   *sinkURL,
		MaxRetries: 4,
		BaseDelay:  50 * time.Millisecond,
		MaxDelay:   1 * time.Second,
	})

	// Start Prometheus /metrics HTTP server
	metricsSrv := &http.Server{
		Addr: *metricsAddr,
		Handler: metrics.NewHandler(func() metrics.Snapshot {
			return metrics.Snapshot{
				ParsedLines:   pool.ParsedCount(),
				ParseFailures: pool.ParseFailures(),
				AlertsFired:   det.AlertCount(),
				ShippedEvents: client.ShippedEvents(),
				FailedBatches: client.FailedBatches(),
				RetryTotal:    client.RetryTotal(),
			}
		}),
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[metrics] server error: %v", err)
		}
	}()

	rawLinesCh := make(chan string, 1024)
	parsedCh := make(chan model.LogEvent, 1024)
	checkedCh := make(chan model.LogEvent, 1024)
	alertsCh := make(chan model.AnomalyAlert, 64)
	batchesCh := make(chan model.BatchPayload, 64)

	// Stage 1: Tailer reads file until ctx is canceled, then closes rawLinesCh
	go func() {
		if err := tl.Stream(ctx, rawLinesCh); err != nil {
			log.Printf("[tailer] error: %v", err)
		}
		close(rawLinesCh)
	}()

	// Stage 2: Worker pool parses lines from rawLinesCh and closes parsedCh when drained
	// Note: pass context.Background() so downstream stages drain remaining buffered items on shutdown
	go pool.Run(context.Background(), rawLinesCh, parsedCh)

	// Stage 3: Anomaly Detector evaluates sliding window and forwards to checkedCh
	go func() {
		det.Run(context.Background(), parsedCh, checkedCh, alertsCh)
		close(alertsCh)
	}()

	// Alert logger goroutine
	var alertWg sync.WaitGroup
	alertWg.Add(1)
	go func() {
		defer alertWg.Done()
		for a := range alertsCh {
			log.Printf("[ALERT] %s (window=%d, err_rate=%.1f%%, p99=%.2fms)",
				a.Reason, a.WindowTotal, a.ErrorRate*100, a.P99Latency)
		}
	}()

	// Stage 4: Micro-batcher groups events into BatchPayloads and closes batchesCh when drained
	go batcher.Run(context.Background(), checkedCh, batchesCh)

	log.Printf("[gologship] started tailing %s (workers=%d, batch=%d, metrics=%s)",
		*filePath, *workers, *batchSize, *metricsAddr)

	// Stage 5: Shipper sends batches over HTTP until batchesCh is completely drained
	for batch := range batchesCh {
		if err := client.SendBatch(context.Background(), batch); err != nil {
			log.Printf("[shipper] batch send failed: %v", err)
		}
	}

	alertWg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = metricsSrv.Shutdown(shutdownCtx)

	fmt.Printf("\n=== gologship Graceful Shutdown Summary ===\n")
	fmt.Printf("Parsed Events  : %d (Failures: %d)\n", pool.ParsedCount(), pool.ParseFailures())
	fmt.Printf("Alerts Fired   : %d\n", det.AlertCount())
	fmt.Printf("Shipped Events : %d (Failed Batches: %d, HTTP Retries: %d)\n",
		client.ShippedEvents(), client.FailedBatches(), client.RetryTotal())
}
