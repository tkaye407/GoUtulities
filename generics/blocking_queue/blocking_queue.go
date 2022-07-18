package generic_blocking_queue

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

// ErrQueueClosed is returned if Close was called before calling PushBack or before calling
// BlockingPopFront on a non-empty queue
var ErrQueueClosed = errors.New("blocking queue is closed")

// BlockingQueue is a FIFO queue which blocks on read when empty.
type BlockingQueue[T any] interface {
	BlockingPopFront(ctx context.Context) (T, error)
	PushFront(data ...T) error
	PushBack(data T) error
	Close()
	Len() int
}

//// NewBlockingQueue returns an empty BlockingQueue
//func NewBlockingQueue[T any]() BlockingQueue {
//	return &blockingQueue[T]{
//		lock:         sync.Mutex{},
//		elemList:     list.New(),
//		readNotEmpty: make(chan struct{}, 1),
//		closedCh:     make(chan struct{}),
//	}
//}

type blockingQueue[T any] struct {
	lock     sync.Mutex
	elemList *list.List

	readNotEmpty chan struct{}

	closedOnce sync.Once
	closedCh   chan struct{}
}

// Close causes future calls to PushBack return ErrQueueClosed and BlockingPopFront to return
// ErrQueueClosed after the queue has been emptied.
func (queue *blockingQueue) Close() {
	queue.closedOnce.Do(func() {
		close(queue.closedCh)
	})
}

// BlockingPopFront returns the item in the front of the queue. If the queue is empty,
// it blocks until new data becomes available or the context is cancelled.
func (queue *blockingQueue[T]) BlockingPopFront(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	for {
		queue.lock.Lock()
		out := queue.elemList.Front()

		// queue wasn't empty, so we can return the item immediately
		if out != nil {
			queue.elemList.Remove(out)
			queue.lock.Unlock()
			return out.Value, nil
		}
		queue.lock.Unlock()

		// queue is empty - go to sleep until something is put into it,
		// or the context is cancelled.
		select {
		case <-queue.readNotEmpty:
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-queue.closedCh:
			return nil, ErrQueueClosed
		}
	}
}

// PushFront puts entries at the front of the queue. If there are any readers blocked on calls to BlockingPopFront(),
// they will wake up and one of them will pop off the new entry and return.
func (queue *blockingQueue[T]) PushFront(data ...T) error {
	select {
	case <-queue.closedCh:
		return ErrQueueClosed
	default:
	}

	queue.lock.Lock()
	defer queue.lock.Unlock()

	for i := len(data) - 1; i >= 0; i-- {
		queue.elemList.PushFront(data[i])
	}

	// wake up blocked readers
	select {
	case queue.readNotEmpty <- struct{}{}:
	default:
	}
	return nil
}

// PushBack puts an entry at the back of the queue. If there are any readers blocked on calls to BlockingPopFront(),
// they will wake up and one of them will pop off the new entry and return.
func (queue *blockingQueue[T]) PushBack(data T) error {
	select {
	case <-queue.closedCh:
		return ErrQueueClosed
	default:
	}

	queue.lock.Lock()
	defer queue.lock.Unlock()
	queue.elemList.PushBack(data)

	// wake up blocked readers
	select {
	case queue.readNotEmpty <- struct{}{}:
	default:
	}
	return nil
}

// Len returns the depth of the queue
func (queue *blockingQueue) Len() int {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.elemList.Len()
}
