package graphdb

import (
	"context"
	"sync"
)

// Task represents a unit of work to be executed by the worker pool.
type Task func() (interface{}, error)

// taskResult holds the result of a completed task.
type taskResult struct {
	Value interface{}
	Err   error
	Index int
}

// workerPool manages a fixed set of goroutines for concurrent query execution.
type workerPool struct {
	workers int
	tasks   chan poolTask
	wg      sync.WaitGroup
	quit    chan struct{}
	once    sync.Once
}

type poolTask struct {
	fn     Task
	result chan<- taskResult
	index  int
}

// newWorkerPool creates and starts a pool with the given number of workers.
func newWorkerPool(size int) *workerPool {
	if size <= 0 {
		size = 1
	}

	p := &workerPool{
		workers: size,
		tasks:   make(chan poolTask, size*4), // buffered for backpressure
		quit:    make(chan struct{}),
	}

	p.wg.Add(size)
	for i := 0; i < size; i++ {
		go p.worker()
	}

	return p
}

// worker is the main loop for a pool goroutine.
func (p *workerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case task, ok := <-p.tasks:
			if !ok {
				return
			}
			val, err := task.fn()
			task.result <- taskResult{Value: val, Err: err, Index: task.index}
		case <-p.quit:
			return
		}
	}
}

// submit sends a task to the pool and returns immediately.
func (p *workerPool) submit(fn Task, resultCh chan<- taskResult, index int) {
	p.tasks <- poolTask{fn: fn, result: resultCh, index: index}
}

// stop gracefully shuts down the worker pool.
func (p *workerPool) stop() {
	p.once.Do(func() {
		close(p.quit)
		close(p.tasks)
		p.wg.Wait()
	})
}

// ExecuteConcurrent runs multiple tasks concurrently using the worker pool
// and returns all results in order. Respects context cancellation.
func (p *workerPool) ExecuteConcurrent(ctx context.Context, tasks []Task) []taskResult {
	n := len(tasks)
	if n == 0 {
		return nil
	}

	resultCh := make(chan taskResult, n)

	// Submit all tasks.
	submitted := 0
	for i, t := range tasks {
		select {
		case <-ctx.Done():
			break
		default:
			p.submit(t, resultCh, i)
			submitted++
		}
	}

	// Collect results.
	results := make([]taskResult, n)
	for i := 0; i < submitted; i++ {
		select {
		case r := <-resultCh:
			results[r.Index] = r
		case <-ctx.Done():
			// Fill remaining with context error.
			for j := i; j < submitted; j++ {
				results[j] = taskResult{Err: ctx.Err()}
			}
			return results
		}
	}

	return results
}
