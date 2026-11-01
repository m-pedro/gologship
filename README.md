# gologship

A high-concurrency Go log tailer, sliding-window anomaly detector, and fault-tolerant HTTP micro-batch shipper with Prometheus observability—built using **100% Go standard library** with zero external dependencies.

## Streaming Pipeline Architecture

```text
[Log File] 
   │ (internal/tailer: EOF polling + copytruncate detection)
   ▼
rawLinesCh (chan string)
   │ (internal/worker: N concurrent zero-alloc parser goroutines)
   ▼
parsedCh (chan model.LogEvent)
   │ (internal/detector: O(1) ring-buffer sliding window -> 5xx & p99 alerts)
   ▼
checkedCh (chan model.LogEvent)
   │ (internal/shipper: dual-trigger count + time.Ticker micro-batcher)
   ▼
batchesCh (chan model.BatchPayload)
   │ (internal/shipper: HTTP POST client w/ Equal-Jitter exponential backoff)
   ▼
[Downstream HTTP Ingestion Sink]  +  [Prometheus /metrics & /healthz (:9090)]
Key Engineering Highlights
Zero-Allocation, Zero-Regex Parsing (internal/model): Uses strings.IndexByte slice indexing to parse HTTP access logs in ~75ns per line with 0 B/op (0 allocs/op).
Bounded Memory Ring Window (internal/detector): Maintains real-time 5xx error rates and p99 tail latencies over a sliding circular buffer with alert cooldown suppression to prevent alert storms.
Resilient Micro-Batching & Backoff (internal/shipper): Flushes batches on either item count (-batch-size) or timer expiration (-flush-interval), retrying transient 429/5xx sink failures with randomized exponential backoff while failing fast on permanent 4xx errors.
Graceful Signal Drain (cmd/gologship): Captures SIGINT/SIGTERM via signal.NotifyContext to stop file tailing cleanly while draining all in-flight worker, detector, and batcher channels before process exit.
Race-Verified Concurrency: Every package and the end-to-end multi-stage pipeline are continuously verified under Go's ThreadSanitizer (go test -race ./...).
Quickstart
1. Stream Synthetic Live Logs (with Simulated 5xx Error Spike)
Bash
python3 scripts/stream_logs.py -n 1000 -r 500 --inject-spike -o data/live_access.log
2. Run gologship Pipeline
Bash
go run ./cmd/gologship \
  -file data/live_access.log \
  -workers 4 \
  -batch-size 100 \
  -flush-interval 500ms \
  -metrics-addr :9090 \
  -run-for 2s
3. Run Race-Detector Tests & Zero-Alloc Benchmarks
Bash
go test -v -race ./...
go test -bench=. -benchmem ./internal/model/...
15-Day Incremental Engineering Roadmap (Month 1, Days 16–30)
[x] Day 16: Initialize Go module, live log stream simulator (scripts/stream_logs.py), and roadmap
[x] Day 17: Implement zero-regex HTTP access log parser (internal/model) and table-driven tests
[x] Day 18: Implement context.Context-aware file tailer with EOF polling (internal/tailer)
[x] Day 19: Add live file-append and truncation tests for streaming tailer
[x] Day 20: Implement bounded fan-out/fan-in goroutine worker pool (internal/worker)
[x] Day 21: Implement ring-buffer sliding window for real-time traffic metrics (internal/detector)
[x] Day 22: Add 5xx error-rate and p99 latency spike anomaly alerts
[x] Day 23: Implement dual-trigger (batch size + time ticker) micro-batcher (internal/shipper)
[x] Day 24: Add HTTP batch sink client with exponential backoff and full jitter
[x] Day 25: Add httptest chaos/fault-injection tests for transient 503 retries
[x] Day 26: Expose lock-free atomic Prometheus /metrics and /healthz endpoints (internal/metrics)
[x] Day 27: Wire full CLI pipeline in cmd/gologship/main.go with SIGINT/SIGTERM graceful drain
[x] Day 28: Add end-to-end race-detector (go test -race) pipeline integration test
[x] Day 29: Add testing.B parser and worker pool throughput benchmarks
[x] Day 30: Publish Month 1 architecture diagrams, throughput benchmarks, and retrospective
