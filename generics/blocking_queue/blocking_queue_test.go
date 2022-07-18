package generic_blocking_queue

import (
	"container/list"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockingQueue(t *testing.T) {
	t.Run("Blocking queue", func(t *testing.T) {
		testitems := []string{"a", "b", "c", "d", "e", "f"}
		q := &blockingQueue[string]{
			lock:         sync.Mutex{},
			elemList:     list.New(),
			readNotEmpty: make(chan struct{}, 1),
			closedCh:     make(chan struct{}),
		}
		for _, item := range testitems {
			assert.Nil(t, q.PushBack(item))
		}

		// popping when the queue has entries return immediately
		for i := 0; i < len(testitems); i++ {
			item, err := q.BlockingPopFront(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, item, testitems[i])
		}

		// after pushing items to the *front*, popping them
		// will see the last pushed items first
		testitems2 := []string{"g", "h", "i", "j", "k", "l"}
		for _, item := range testitems2 {
			assert.Nil(t, q.PushFront(item))
		}
		for i := len(testitems2) - 1; i >= 0; i-- {
			item, err := q.BlockingPopFront(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, item, testitems2[i])
		}

		chan1, chan2 := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(chan1)
			// popping an empty queue will block
			d, err := q.BlockingPopFront(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, d, "test!")
		}()

		// Pushing unblocks the blocked reader
		assert.Nil(t, q.PushBack("test!"))
		<-chan1

		// Pushing many to the front also unblocks a blocked reader
		chan1 = make(chan struct{})
		go func() {
			defer close(chan1)
			// popping an empty queue will block
			d, err := q.BlockingPopFront(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, d, "a")

			// Should be another one available immediately
			d, err = q.BlockingPopFront(context.Background())
			assert.Nil(t, err)
			assert.Equal(t, d, "b")
		}()

		// Pushing unblocks the blocked reader
		assert.Nil(t, q.PushFront("a", "b"))
		<-chan1

		ctx, cancelFunc := context.WithCancel(context.Background())
		go func() {
			defer close(chan2)
			// blocked pop will unblock when context is canceled
			d, err := q.BlockingPopFront(ctx)
			assert.Equal(t, err, context.Canceled)
			assert.Nil(t, d)
		}()

		// Canceling context also unblocks a blocked reader
		cancelFunc()
		<-chan2

		assert.Nil(t, q.PushBack("a"))
		assert.Nil(t, q.PushBack("b"))
		assert.Nil(t, q.PushBack("c"))
		ctx, cancelFunc = context.WithCancel(context.Background())
		cancelFunc()

		// Popping with a canceled context should immediately return error
		_, err := q.BlockingPopFront(ctx)
		assert.Equal(t, err, context.Canceled)
	})

	//t.Run("close blocking queue", func(t *testing.T) {
	//	q := NewBlockingQueue()
	//	assert.Nil(t, q.PushBack("foo"))
	//	q.Close()
	//
	//	assert.Equal(t, q.PushBack("bar"), ErrQueueClosed)
	//
	//	item, err := q.BlockingPopFront(context.Background())
	//	assert.Nil(t, err)
	//	assert.Equal(t, item.(string), "foo")
	//
	//	item, err = q.BlockingPopFront(context.Background())
	//	assert.Nil(t, item)
	//	assert.Equal(t, err, ErrQueueClosed)
	//})

}
