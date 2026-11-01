package worker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/m-pedro/gologship/internal/model"
)

type Pool struct {
	numWorkers    int
	parsedCount   atomic.Uint64
	parseFailures atomic.Uint64
}

func NewPool(numWorkers int) *Pool {
	if numWorkers <= 0 {
		numWorkers = 4
	}
	return &Pool{numWorkers: numWorkers}
}

func (p *Pool) Run(ctx context.Context, in <-chan string, out chan<- model.LogEvent) {
	var wg sync.WaitGroup
	wg.Add(p.numWorkers)

	for i := 0; i < p.numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case line, ok := <-in:
					if !ok {
						return
					}
					ev, err := model.ParseLine(line)
					if err != nil {
						p.parseFailures.Add(1)
						continue
					}
					p.parsedCount.Add(1)
					select {
					case out <- ev:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	close(out)
}

func (p *Pool) ParsedCount() uint64 {
	return p.parsedCount.Load()
}

func (p *Pool) ParseFailures() uint64 {
	return p.parseFailures.Load()
}
