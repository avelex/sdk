package dispatcher

import (
	"log/slog"
	"sync"
	"sync/atomic"
)

type worker[T any] struct {
	wg     *sync.WaitGroup
	closed atomic.Bool
	queue  chan T
}

func newWorker[T any](limit int) *worker[T] {
	return &worker[T]{
		wg:     &sync.WaitGroup{},
		closed: atomic.Bool{},
		queue:  make(chan T, limit),
	}
}

func (w *worker[T]) Start() {
	w.wg.Add(1)
}

func (w *worker[T]) Done() {
	w.wg.Done()
}

func (w *worker[T]) Send(v T) {
	if w.closed.Load() {
		return
	}

	w.queue <- v
}

func (w *worker[T]) Queue() <-chan T {
	return w.queue
}

func (w *worker[T]) Limit() int {
	return cap(w.queue)
}

func (w *worker[T]) Close() {
	if !w.closed.CompareAndSwap(false, true) {
		return
	}

	close(w.queue)
	w.wg.Wait()

	slog.Debug("[X] Worker closed")
}
