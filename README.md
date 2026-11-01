# gologship

A high-concurrency Go log tailer, sliding-window anomaly detector, and fault-tolerant HTTP micro-batch shipper with Prometheus observability—built using **100% Go standard library** with zero external dependencies.

## Repo Structure
```text
gologship/
├── go.mod                        # Zero external dependencies (100% Go stdlib)
├── README.md                     # Architecture, directory layout, benchmarks & roadmap
├── scripts/
│   └── stream_logs.py            # Live log stream simulator with 5xx spike injection
├── cmd/
│   └── gologship/
│       └── main.go               # CLI flags, SIGINT/SIGTERM drain & pipeline wiring
└── internal/
    ├── pipeline_test.go          # End-to-end multi-stage race-detector integration test
    ├── model/
    │   ├── record.go             # LogEvent, AnomalyAlert, and BatchPayload schemas
    │   ├── parser.go             # Zero-allocation, zero-regex HTTP log slice parser
    │   ├── parser_test.go        # Table-driven parser unit & edge-case tests
    │   └── bench_test.go         # Single-core and parallel zero-alloc benchmarks
    ├── tailer/
    │   ├── tailer.go             # Context-aware streaming file follower & rotation handler
    │   └── tailer_test.go        # Live file append & copytruncate rotation tests
    ├── worker/
    │   ├── pool.go               # Bounded fan-out/fan-in goroutine parsing worker pool
    │   └── pool_test.go          # Concurrent worker pool race-safety tests
    ├── detector/
    │   ├── window.go             # O(1) ring-buffer sliding window for error rate & p99
    │   ├── detector.go           # Streaming anomaly evaluator with cooldown suppression
    │   └── window_test.go        # Deterministic clock spike & eviction tests
    ├── shipper/
    │   ├── batcher.go            # Dual-trigger (item count + time.Ticker) micro-batcher
    │   ├── batcher_test.go       # Batch size, ticker flush, and shutdown drain tests
    │   ├── client.go             # HTTP POST sink client with Equal-Jitter backoff
    │   └── client_test.go        # httptest chaos server (503 retry recovery & 400 fast-fail)
    └── metrics/
        ├── server.go             # Lock-free atomic Prometheus /metrics & /healthz server
        └── server_test.go        # Endpoint scraping & exposition format tests
```

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
```
## Key Engineering Highlights
```text
Zero-Allocation, Zero-Regex Parsing (internal/model): Uses strings.IndexByte slice indexing to parse HTTP access logs in ~75ns per line with 0 B/op (0 allocs/op).
Bounded Memory Ring Window (internal/detector): Maintains real-time 5xx error rates and p99 tail latencies over a sliding circular buffer with alert cooldown suppression to prevent alert storms.
Resilient Micro-Batching & Backoff (internal/shipper): Flushes batches on either item count (-batch-size) or timer expiration (-flush-interval), retrying transient 429/5xx sink failures with randomized exponential backoff while failing fast on permanent 4xx errors.
Graceful Signal Drain (cmd/gologship): Captures SIGINT/SIGTERM via signal.NotifyContext to stop file tailing cleanly while draining all in-flight worker, detector, and batcher channels before process exit.
Race-Verified Concurrency: Every package and the end-to-end multi-stage pipeline are continuously verified under Go's ThreadSanitizer (go test -race ./...).
```
## Quickstart
1. Stream Synthetic Live Logs (with Simulated 5xx Error Spike)
```bash
python3 scripts/stream_logs.py -n 1000 -r 500 --inject-spike -o data/live_access.log
```
2. Run gologship Pipeline
```bash
go run ./cmd/gologship \
  -file data/live_access.log \
  -workers 4 \
  -batch-size 100 \
  -flush-interval 500ms \
  -metrics-addr :9090 \
  -run-for 2s
```
3. Run Race-Detector Tests & Zero-Alloc Benchmarks
```bash
go test -v -race ./...
go test -bench=. -benchmem ./internal/model/...
```
## 15-Day Incremental Engineering Roadmap (Month 1, Days 16–30)
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
