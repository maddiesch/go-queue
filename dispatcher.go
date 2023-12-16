package queue

type Dispatcher interface {
	Push(Job) (JobID, error)
}
