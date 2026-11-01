package shipper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/m-pedro/gologship/internal/model"
)

// ClientConfig configures the retryable HTTP batch shipper.
type ClientConfig struct {
	Endpoint   string
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	HTTPClient *http.Client
}

// Client ships BatchPayloads over HTTP POST with exponential backoff and jitter.
type Client struct {
	cfg           ClientConfig
	shippedEvents atomic.Uint64
	failedBatches atomic.Uint64
	retryTotal    atomic.Uint64
}

func NewClient(cfg ClientConfig) *Client {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 4
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 50 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 2 * time.Second
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Client{cfg: cfg}
}

// SendBatch marshals payload to JSON and POSTs it to the configured sink.
// Retries on network errors, 429 Too Many Requests, and 5xx server errors.
func (c *Client) SendBatch(ctx context.Context, payload model.BatchPayload) error {
	if c.cfg.Endpoint == "" {
		c.shippedEvents.Add(uint64(payload.Count))
		return nil
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			c.retryTotal.Add(1)
			backoff := c.computeBackoffWithJitter(attempt)
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				c.failedBatches.Add(1)
				return ctx.Err()
			case <-timer.C:
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(body))
		if err != nil {
			c.failedBatches.Add(1)
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			c.shippedEvents.Add(uint64(payload.Count))
			return nil
		}

		// Permanent 4xx client errors (except 429) should fail immediately without retrying
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			c.failedBatches.Add(1)
			return fmt.Errorf("permanent HTTP client error: status %d", resp.StatusCode)
		}

		lastErr = fmt.Errorf("transient server error: status %d", resp.StatusCode)
	}

	c.failedBatches.Add(1)
	if lastErr == nil {
		lastErr = errors.New("exhausted retries")
	}
	return lastErr
}

// computeBackoffWithJitter implements AWS-style "Equal Jitter" exponential backoff.
func (c *Client) computeBackoffWithJitter(attempt int) time.Duration {
	shift := attempt - 1
	if shift > 10 {
		shift = 10
	}
	ceil := c.cfg.BaseDelay * time.Duration(1<<shift)
	if ceil > c.cfg.MaxDelay || ceil <= 0 {
		ceil = c.cfg.MaxDelay
	}
	half := ceil / 2
	if half <= 0 {
		return ceil
	}
	return half + time.Duration(rand.Int64N(int64(half)))
}

func (c *Client) ShippedEvents() uint64 {
	return c.shippedEvents.Load()
}

func (c *Client) FailedBatches() uint64 {
	return c.failedBatches.Load()
}

func (c *Client) RetryTotal() uint64 {
	return c.retryTotal.Load()
}
