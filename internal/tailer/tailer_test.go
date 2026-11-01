package tailer

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTailer_StreamsLiveAppendsAndTruncation(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "access.log")

	f, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("failed to create temp log: %v", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out := make(chan string, 16)
	tl := New(Config{
		FilePath:     logPath,
		PollInterval: 10 * time.Millisecond,
		StartAtEnd:   false,
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- tl.Stream(ctx, out)
	}()

	_, _ = f.WriteString("10.0.0.1 - - [25/Oct/2026:00:00:01 +0000] ")
	_ = f.Sync()
	time.Sleep(25 * time.Millisecond)
	_, _ = f.WriteString("\"GET /first HTTP/1.1\" 200 100 1.2\n")
	_ = f.Sync()

	select {
	case got := <-out:
		if got != `10.0.0.1 - - [25/Oct/2026:00:00:01 +0000] "GET /first HTTP/1.1" 200 100 1.2` {
			t.Fatalf("unexpected first line: %s", got)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for first line")
	}

	_ = f.Truncate(0)
	_, _ = f.Seek(0, 0)
	_, _ = f.WriteString("10.0.0.2 - - [25/Oct/2026:00:00:02 +0000] \"GET /rotated HTTP/1.1\" 200 64 0.8\n")
	_ = f.Sync()

	select {
	case got := <-out:
		if got != `10.0.0.2 - - [25/Oct/2026:00:00:02 +0000] "GET /rotated HTTP/1.1" 200 64 0.8` {
			t.Fatalf("unexpected line after truncation: %s", got)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for line after file truncation")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("tailer returned unexpected error: %v", err)
	}
}
