# gologship

A high-concurrency Go log tailer, sliding-window anomaly detector, and fault-tolerant HTTP micro-batch shipper with Prometheus observability.

## 15-Day Incremental Engineering Roadmap (Month 1, Days 16–30)

- [x] **Day 16:** Initialize Go module, live log stream simulator (`scripts/stream_logs.py`), and roadmap
- [ ] **Day 17:** Implement zero-regex HTTP access log parser (`internal/model`) and table-driven tests
- [ ] **Day 18:** Implement `context.Context`-aware file tailer with EOF polling (`internal/tailer`)
- [ ] **Day 19:** Add live file-append and truncation tests for streaming tailer
- [ ] **Day 20:** Implement bounded fan-out/fan-in goroutine worker pool (`internal/worker`)
- [ ] **Day 21:** Implement ring-buffer sliding window for real-time traffic metrics (`internal/detector`)
- [ ] **Day 22:** Add 5xx error-rate and p99 latency spike anomaly alerts
- [ ] **Day 23:** Implement dual-trigger (batch size + time ticker) micro-batcher (`internal/shipper`)
- [ ] **Day 24:** Add HTTP batch sink client with exponential backoff and full jitter
- [ ] **Day 25:** Add `httptest` chaos/fault-injection tests for transient `503` retries
- [ ] **Day 26:** Expose lock-free atomic Prometheus `/metrics` and `/healthz` endpoints (`internal/metrics`)
- [ ] **Day 27:** Wire full CLI pipeline in `cmd/gologship/main.go` with `SIGINT`/`SIGTERM` graceful drain
- [ ] **Day 28:** Add end-to-end race-detector (`go test -race`) pipeline integration test
- [ ] **Day 29:** Add `testing.B` parser and worker pool throughput benchmarks
- [ ] **Day 30:** Publish Month 1 architecture diagrams, throughput benchmarks, and retrospective
