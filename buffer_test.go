package blockstream

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBufferDynamic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := newBuffer(ctx, 8, 256)
	bs, ids := randBlocks(t, rand.Reader, 256, 256)
	buf.Order() <- ids
	close(buf.Order())

	// Check that buffer is unbounded and blocks can be written without deadlocking.
	for _, b := range bs {
		select {
		case buf.Input() <- b:
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Buffer input is blocked.")
		}
	}
}

func TestBufferOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlocks(t, rand.Reader, 32, 256)
	buf := newBuffer(ctx, 32, 32)
	buf.Order() <- ids
	close(buf.Order())

	go func() {
		for i := len(bs) - 1; i >= 0; i-- { // send blocks in reverse
			buf.Input() <- bs[i]
		}
	}()

	var i int
	for b := range buf.Output() {
		assert.Equal(t, ids[i], b.Cid()) // check original order
		i++
	}
}

func TestBufferClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	buf := newBuffer(ctx, 32, 32)
	bs, ids := randBlocks(t, rand.Reader, 1, 256)
	buf.Order() <- ids
	buf.Input() <- bs[0]
	close(buf.Order())

	for b := range buf.Output() { // check that serves one block and closed after Order is closed
		assert.Equal(t, bs[0], b)
	}

	buf = newBuffer(ctx, 32, 32)
	buf.Order() <- ids
	buf.Input() <- bs[0]
	cancel()

	_, ok := <-buf.Output()
	assert.False(t, ok) // check that closing context terminates Buffer
}
