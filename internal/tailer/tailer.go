package tailer

import (
	"bufio"
	"context"
	"io"
	"os"
	"strings"
	"time"
)

type Config struct {
	FilePath     string
	PollInterval time.Duration
	StartAtEnd   bool
}

type Tailer struct {
	cfg Config
}

func New(cfg Config) *Tailer {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 25 * time.Millisecond
	}
	return &Tailer{cfg: cfg}
}

func (t *Tailer) Stream(ctx context.Context, out chan<- string) error {
	f, err := os.Open(t.cfg.FilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	var offset int64
	if t.cfg.StartAtEnd {
		offset, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
	}

	reader := bufio.NewReaderSize(f, 64*1024)
	var partial strings.Builder

	ticker := time.NewTicker(t.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			offset += int64(len(line))
			if strings.HasSuffix(line, "\n") {
				fullLine := line
				if partial.Len() > 0 {
					partial.WriteString(line)
					fullLine = partial.String()
					partial.Reset()
				}
				fullLine = strings.TrimRight(fullLine, "\r\n")
				if len(fullLine) > 0 {
					select {
					case out <- fullLine:
					case <-ctx.Done():
						return nil
					}
				}
			} else {
				partial.WriteString(line)
			}
		}

		if err == io.EOF {
			if info, statErr := f.Stat(); statErr == nil && info.Size() < offset {
				offset, _ = f.Seek(0, io.SeekStart)
				reader.Reset(f)
				partial.Reset()
				continue
			}

			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				continue
			}
		} else if err != nil {
			return err
		}
	}
}
