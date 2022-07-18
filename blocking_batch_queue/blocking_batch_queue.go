package blocking_batch_queue

import (
	"container/list"
	"context"
	"sync"
)

// BatchQueueItem represents information about an item of work that needs to be consumed from the BlockingBatchQueue
// It is mapped to a key specified by the Key() field, and the Merge() function is used
// to concatenate two items that operate on the same key.
type BatchQueueItem interface {
	Key() interface{}
	Merge(other BatchQueueItem) BatchQueueItem
}

type blockingBatchQueue struct {
	lock     sync.Mutex
	elemList *list.List
	elemMap  map[interface{}]*list.Element

	readNotEmpty chan struct{}
}

// NewBlockingBatchQueue returns a new, empty BlockingBatchQueue.
func NewBlockingBatchQueue() BlockingBatchQueue {
	return &blockingBatchQueue{
		lock:         sync.Mutex{},
		elemList:     list.New(),
		elemMap:      map[interface{}]*list.Element{},
		readNotEmpty: make(chan struct{}, 1),
	}
}

// BlockingBatchQueue is a queue for tracking items of work that can be keyed on a specific value (partition, ident, etc)
type BlockingBatchQueue interface {
	BlockingPopFront(ctx context.Context) BatchQueueItem
	PopFront() (BatchQueueItem, bool)
	NotEmptyChan() <-chan struct{}
	PushBack(notification ...BatchQueueItem)
	Len() int
}

func (queue *blockingBatchQueue) Len() int {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	return queue.elemList.Len()
}

// BlockingPopFront returns the item in the front of the queue. If the queue is empty,
// it blocks until new data becomes available or the context is cancelled.
func (queue *blockingBatchQueue) BlockingPopFront(ctx context.Context) BatchQueueItem {
	for {
		queue.lock.Lock()
		out := queue.elemList.Front()

		// queue wasn't empty, so we can return the item immediately
		if out != nil {
			queue.elemList.Remove(out)
			val := out.Value.(BatchQueueItem)
			delete(queue.elemMap, val.Key())
			queue.lock.Unlock()
			return val
		}
		queue.lock.Unlock()

		// queue is empty - go to sleep until something is put into it,
		// or the context is cancelled.
		select {
		case <-queue.readNotEmpty:
			continue
		case <-ctx.Done():
			return nil
		}
	}
}

// PopFront returns the item at the front of the queue, and false if the queue was empty.
func (queue *blockingBatchQueue) PopFront() (BatchQueueItem, bool) {
	queue.lock.Lock()
	defer queue.lock.Unlock()
	out := queue.elemList.Front()
	if out == nil {
		return nil, false
	}
	queue.elemList.Remove(out)
	notification := out.Value.(BatchQueueItem)
	delete(queue.elemMap, notification.Key())
	return notification, true
}

// NotEmptyChan returns a channel that immediately resolves if the queue has any items
// otherwise it returns a channel that will signal when something is added to the queue
// Note that the channel may return a value but the queue may still be empty when checked if
// another concurrent reader has already popped off the new entry
func (queue *blockingBatchQueue) NotEmptyChan() <-chan struct{} {
	if queue.Len() > 0 {
		itemChan := make(chan struct{})
		close(itemChan)
		return itemChan
	}

	return queue.readNotEmpty
}

// PushBack attempts to place an entry for each of the given keys at the end
// of the queue. If the queue already contains an entry for a key, it will replace
// the entry with the result of calling existingEntry.Merge(newEntry).
func (queue *blockingBatchQueue) PushBack(notifications ...BatchQueueItem) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	for _, notification := range notifications {
		key := notification.Key()
		item, ok := queue.elemMap[key]
		if ok {
			val := item.Value.(BatchQueueItem)
			item.Value = val.Merge(notification)
		} else {
			newElem := queue.elemList.PushBack(notification)
			queue.elemMap[key] = newElem
		}
	}

	// wake up one blocked reader
	select {
	case queue.readNotEmpty <- struct{}{}:
	default:
	}
}
