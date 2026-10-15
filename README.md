# inas

[![Go Reference](https://pkg.go.dev/badge/github.com/rlsvr/inas.svg)](https://pkg.go.dev/github.com/rlsvr/inas)
[![CI](https://github.com/rlsvr/inas/actions/workflows/ci.yml/badge.svg)](https://github.com/rlsvr/inas/actions/workflows/ci.yml)
[![golangci-lint](https://img.shields.io/badge/golangci--lint-passing-brightgreen?logo=go)](https://github.com/rlsvr/inas/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**`inas`** (Hungarian for *apprentice / helper / runner*) is a lean, zero-dependency Go library providing generic in-process concurrency primitives: bounded FIFO queues and decoupled worker pools.

Designed with **zero external dependencies** (standard library only), `inas` serves as the high-performance building block for Go backends, background worker pipelines, and event streaming systems like [`blazesse`](https://github.com/rlsvr/blazesse).

---

## Primitives

* **`pkg/queue`**: Bounded, generic FIFO queue (`Queue[T]`) supporting blocking `Push` / `Pop` with context cancellation, non-blocking `TryPush` (< 50ns), raw channel `select` integration (`Recv()`), and graceful drain on close.
* **`pkg/worker`**: Generic worker pool (`Run[T]`, `Pool`) that drains a `Queue[T]` across $N$ goroutines with clean lifecycle management (`Wait()`, `Stopped()`).

---

## Installation

```bash
go get github.com/rlsvr/inas
```

---

## Quickstart

### 1. Bounded FIFO Queue (`pkg/queue`)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rlsvr/inas/pkg/queue"
)

func main() {
	// Create a bounded queue of strings with capacity 100
	q := queue.New[string](100)
	defer q.Close()

	ctx := context.Background()

	// Non-blocking push
	if q.TryPush("job-1") {
		fmt.Println("job-1 queued")
	}

	// Blocking push with context cancellation
	_ = q.Push(ctx, "job-2")

	// Blocking pop
	item, _ := q.Pop(ctx)
	fmt.Println("popped:", item)
}
```

---

### 2. Worker Pool (`pkg/worker`)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rlsvr/inas/pkg/queue"
	"github.com/rlsvr/inas/pkg/worker"
)

func main() {
	q := queue.New[int](100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spawn 4 worker goroutines consuming from the queue
	pool := worker.Run(ctx, 4, q, func(ctx context.Context, jobID int) {
		fmt.Printf("Worker processing job %d\n", jobID)
		time.Sleep(10 * time.Millisecond)
	})

	for i := 1; i <= 20; i++ {
		_ = q.Push(ctx, i)
	}

	// Close queue to stop accepting new work and let buffered jobs drain
	q.Close()

	// Wait for in-flight work to complete
	_ = pool.Wait(context.Background())
	fmt.Println("All workers drained and stopped cleanly.")
}
```

---

## Testing & Benchmarks

```bash
# Run all tests with race detector
make test

# Run benchmarks
make bench
```

Benchmark results on Intel® Core™ i5-13600K:
```
BenchmarkQueue_TryPushPop-20   16124436   71.5 ns/op   0 B/op   0 allocs/op
```

---

## License

[MIT](LICENSE) © [rlsvr](https://github.com/rlsvr)
