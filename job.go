package queue

import "context"

type JobID uint64

type Job interface {
	Execute(context.Context) error
}

type JobFunc func(context.Context) error

func (fn JobFunc) Execute(ctx context.Context) error {
	return fn(ctx)
}

var _ Job = (*JobFunc)(nil)
