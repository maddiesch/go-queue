package queue_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maddiesch/go-queue"
	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	t.Run("PushFunc", func(t *testing.T) {
		q := queue.New(queue.Config{
			MaxDepth: 10,
			Logger:   logger,
		})
		expect := Expect("Job Executed")

		q.PushFunc(func(context.Context) error {
			expect.Fullfill()

			return nil
		})

		assert.Equal(t, 1, q.Len())

		shutdown := q.Start()
		t.Cleanup(shutdown)

		expect.Assert(t, time.Second)
	})
}

func BenchmarkQueue(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	b.Run("PushFunc", func(b *testing.B) {
		q := queue.New(queue.Config{
			MaxDepth: b.N,
			Logger:   logger,
		})

		fn := func(ctx context.Context) error {
			return nil
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q.PushFunc(fn)
		}

		b.ReportAllocs()
	})

	b.Run("Push", func(b *testing.B) {
		q := queue.New(queue.Config{
			MaxDepth: b.N,
			Logger:   logger,
		})

		fn := queue.JobFunc(func(ctx context.Context) error {
			return nil
		})

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q.Push(fn)
		}

		b.ReportAllocs()
	})

	b.Run("Execute", func(b *testing.B) {
		q := queue.New(queue.Config{
			MaxDepth: b.N,
			Logger:   logger,
		})

		fn := queue.JobFunc(func(ctx context.Context) error {
			return nil
		})

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			q.Execute(fn)
		}

		b.ReportAllocs()
	})
}

func Expect(name string) *Expectation {
	return &Expectation{name, new(atomic.Bool)}
}

type Expectation struct {
	name string
	done *atomic.Bool
}

func (e *Expectation) Fullfill() {
	e.done.Store(true)
}

func (e *Expectation) Assert(t assert.TestingT, timeout time.Duration) bool {
	return assert.Eventually(t, func() bool { return e.done.Load() }, timeout, 10*time.Millisecond, "Expectation '%s' did not fullfill in %v", e.name, timeout)
}
