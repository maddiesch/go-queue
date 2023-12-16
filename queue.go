package queue

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Queue struct {
	logger       *slog.Logger
	jobID        *atomic.Uint64
	workID       *atomic.Uint64
	waiter       sync.WaitGroup
	telemetry    *sync.Map
	tasks        chan *task
	kill         chan struct{}
	panicHandler *atomic.Value
	errorHandler *atomic.Value
}

type queuePanicHandler func(*task, any)
type queueErrorHandler func(*task, error)

type Config struct {
	MaxDepth int
	Logger   *slog.Logger
}

func New(config Config) *Queue {
	q := &Queue{
		logger:       config.Logger,
		jobID:        new(atomic.Uint64),
		workID:       new(atomic.Uint64),
		telemetry:    new(sync.Map),
		tasks:        make(chan *task, config.MaxDepth),
		kill:         make(chan struct{}),
		panicHandler: new(atomic.Value),
		errorHandler: new(atomic.Value),
	}

	q.panicHandler.Store(queuePanicHandler(func(*task, any) {}))
	q.errorHandler.Store(queueErrorHandler(func(*task, error) {}))

	return q
}

func (q *Queue) SetPanicHandler(fn func(JobID, Job, any)) {
	q.panicHandler.Store(
		queuePanicHandler(func(t *task, a any) {
			fn(t.id, t.job, a)
		}),
	)
}

func (q *Queue) SetErrorHandler(fn func(JobID, Job, error)) {
	q.errorHandler.Store(
		queueErrorHandler(func(t *task, err error) {
			fn(t.id, t.job, err)
		}),
	)
}

func (q *Queue) Push(job Job) (JobID, error) {
	task := q.createTask(job)

	q.tasks <- task

	return task.id, nil
}

func (q *Queue) PushFunc(fn JobFunc) (JobID, error) {
	return q.Push(fn)
}

func (q *Queue) createTask(job Job) *task {
	t := &task{
		id:  JobID(q.jobID.Add(1)),
		job: job,
	}

	q.telemetry.Store(t.id, JobTelemetry{Status: JobStatusPending})

	return t
}

func (q *Queue) Execute(job Job) {
	logger := q.logger.WithGroup("direct")
	task := q.createTask(job)
	q.execute(0, logger, task)
}

func (q *Queue) execute(id uint64, logger *slog.Logger, t *task) {
	tLogger := logger.WithGroup("task").With(
		slog.Uint64("id", uint64(t.id)),
	)

	q.telemetry.Store(t.id, JobTelemetry{Status: JobStatusRunning})

	defer func() {
		if r := recover(); r != nil {
			tLogger.Error("Task Panic Recovery Handler", slog.Any("panic", r))
			q.panicHandler.Load().(queuePanicHandler)(t, r)
			q.telemetry.Store(t.id, JobTelemetry{Status: JobStatusFinished, Panic: r})
		}
	}()

	tLogger.Debug("Executing Task")

	ctx := context.Background()

	startTime := time.Now()
	err := t.job.Execute(ctx)
	runtime := time.Since(startTime)

	q.telemetry.Store(t.id, JobTelemetry{Status: JobStatusFinished, Error: err})

	if err != nil {
		tLogger.Error("Task Returned Error", slog.Any("error", err))
		q.errorHandler.Load().(queueErrorHandler)(t, err)
	}

	tLogger.DebugContext(ctx, "Task Completed", slog.Duration("runtime", runtime))
}

func (q *Queue) Start() func() {
	id := q.workID.Add(1)
	shutdown := make(chan struct{})
	workerLog := q.logger.WithGroup("worker").With(slog.Uint64("id", id))

	q.waiter.Add(1)

	go func() {
		defer q.waiter.Done()

		workerLog.Debug("Starting Worker")

		defer func() {
			workerLog.DebugContext(context.Background(), "Worker Shutdown")
		}()

		for {
			select {
			case <-shutdown:
				return
			case <-q.kill:
				return
			case task := <-q.tasks:
				q.execute(id, workerLog, task)
			}
		}
	}()

	return func() {
		close(shutdown)
	}
}

func (q *Queue) Len() int {
	return len(q.tasks)
}

func (q *Queue) Wait() {
	q.waiter.Wait()
}

type task struct {
	id  JobID
	job Job
}

var _ Dispatcher = (*Queue)(nil)

type JobStatus uint8

const (
	_ JobStatus = iota
	JobStatusPending
	JobStatusRunning
	JobStatusFinished
)

type JobTelemetry struct {
	Status JobStatus
	Panic  any   `json:",omitempty"`
	Error  error `json:",omitempty"`
}
