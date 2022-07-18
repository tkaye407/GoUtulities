package blocking_batch_queue

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testNotification struct {
	key  int
	size int
	work []string
}

func (notification *testNotification) Key() interface{} {
	return notification.key
}

func (notification *testNotification) Size() int {
	return notification.size
}

func (notification *testNotification) Merge(other BatchQueueItem) BatchQueueItem {
	typedOther := other.(*testNotification)
	notification.work = append(notification.work, typedOther.work...)
	return notification
}

func TestNotifyQueue(t *testing.T) {
	t.Run("popping from empty queue should return false", func(t *testing.T) {
		q := NewBlockingBatchQueue()
		_, ok := q.PopFront()
		assert.False(t, ok)
	})

	t.Run("popping from non-empty queue should return entries in sequence", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		for index, k := range []int{1, 2, 3, 4} {
			q.PushBack(&testNotification{
				key:  k,
				size: index,
				work: []string{fmt.Sprintf("%d", k)},
			})
		}

		// pop all the entries back out and verify that they match
		for index, k := range []int{1, 2, 3, 4} {
			v, ok := q.PopFront()
			assert.True(t, ok)
			assert.Equal(t, v, &testNotification{
				key:  k,
				size: index,
				work: []string{fmt.Sprintf("%d", k)},
			})
		}

		// we popped all the elements, so verify that the queue is now empty
		_, ok := q.PopFront()
		assert.False(t, ok)
	})

	t.Run("pushing to queue with a key that is already in the list should update work for that key", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		for index, k := range []int{1, 2, 3, 4} {
			q.PushBack(&testNotification{
				key:  k,
				size: index,
				work: []string{fmt.Sprintf("%d", k)},
			})
		}

		// Load up the queue again but use the same keys.
		// This should keep the queue at the same size but just update the version numbers.
		// also, push using the variadic args feature of PushBack this time
		notifications := make([]BatchQueueItem, 4)

		for index, k := range []int{1, 2, 3, 4} {
			notifications[index] = &testNotification{
				key:  k,
				size: index * 3,
				work: []string{fmt.Sprintf("%d", 10*k)},
			}
		}
		q.PushBack(notifications...)

		// pop all the entries back out and verify that they match
		for index, k := range []int{1, 2, 3, 4} {
			v, ok := q.PopFront()
			assert.True(t, ok)
			assert.Equal(t, v, &testNotification{
				key:  k,
				size: index,
				work: []string{fmt.Sprintf("%d", k), fmt.Sprintf("%d", 10*k)},
			})
		}

		// Queue should be empty
		_, ok := q.PopFront()
		assert.False(t, ok)
	})

	t.Run("blocking pop from a non-empty queue should return immediately", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		q.PushBack(&testNotification{
			key:  1,
			size: 10,
			work: []string{"a"},
		})
		next := q.BlockingPopFront(context.Background())
		assert.Equal(t, next, &testNotification{
			key:  1,
			size: 10,
			work: []string{"a"},
		})

		// Queue should be empty
		_, ok := q.PopFront()
		assert.False(t, ok)
	})

	t.Run("blocking pop from empty queue should abort when context is cancelled", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		doneChan := make(chan struct{})
		ctx, cancelFunc := context.WithCancel(context.Background())
		go func() {
			val := q.BlockingPopFront(ctx)
			assert.Nil(t, val)
			close(doneChan)
		}()

		cancelFunc()

		<-doneChan
	})

	t.Run("blocking pop from an empty queue should block until an item is placed in the queue", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		itemChan := make(chan BatchQueueItem)
		go func() {
			next := q.BlockingPopFront(context.Background())
			itemChan <- next
		}()

		q.PushBack(&testNotification{
			key:  123,
			size: 100,
			work: []string{"abc"},
		})

		item := <-itemChan
		assert.Equal(t, item, &testNotification{
			key:  123,
			size: 100,
			work: []string{"abc"},
		})
	})

	t.Run("QueueNotEmptyChan when the queue has elements should return already closed channel", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		q.PushBack(&testNotification{
			key:  123,
			size: 100,
			work: []string{"abc"},
		})

		queueNotEmpty := q.NotEmptyChan()
		<-queueNotEmpty
	})

	t.Run("QueueNotEmptyChan when the queue has no elements send to channel when element is added to queue", func(t *testing.T) {
		q := NewBlockingBatchQueue()

		notEmptyChan := make(chan struct{})
		go func() {
			next := q.NotEmptyChan()
			<-next
			notEmptyChan <- struct{}{}
		}()

		select {
		case <-notEmptyChan:
			t.Fatalf("not empty channel immediately unblocked")
		case <-time.After(10 * time.Millisecond):
			// continue
		}

		q.PushBack(&testNotification{
			key:  123,
			size: 100,
			work: []string{"abc"},
		})

		<-notEmptyChan
	})
}
