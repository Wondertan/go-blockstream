package blockstream

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferDynamic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewBuffer(ctx, 8, 256)
	defer buf.Close()

	bs, ids := randBlocks(t, rand.Reader, 256, 256)
	go func() {
		err := buf.Order(ids...)
		require.Nil(t, err, err)
	}()

	tmr := time.NewTimer(10 * time.Millisecond)
	defer tmr.Stop()

	// Check that buffer is unbounded and blocks can be written without deadlocking.
	for _, b := range bs {
		select {
		case buf.Input() <- b:
			tmr.Reset(10 * time.Millisecond)
		case <-tmr.C:
			t.Fatal("Buffer input is blocked.")
		}
	}
}

func TestBufferOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewBuffer(ctx, 32, 32)
	defer buf.Close()
	bs, ids := randBlocks(t, rand.Reader, 32, 256)

	go func() {
		// define order in new routine
		err := buf.Order(ids...)
		require.Nil(t, err, err)

		// send blocks in reverse
		for i := len(bs) - 1; i >= 0; i-- {
			buf.Input() <- bs[i]
		}
	}()

	// check requested order
	for i := 0; i < len(bs)-1; i++ {
		select {
		case b := <-buf.Output():
			assert.Equal(t, ids[i], b.Cid())
		}
	}
	assert.Equal(t, buf.order.Len(), uint32(0))
}

func TestBufferClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bs, ids := randBlocks(t, rand.Reader, 1, 256)

	t.Run("WithClose", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)
		err := buf.Order(ids...)
		require.Nil(t, err, err)

		err = buf.Close()
		require.Nil(t, err, err)

		buf.Input() <- bs[0]
		for b := range buf.Output() { // check that still outputs block and closes Output
			assert.Equal(t, bs[0], b)
		}
	})

	t.Run("WithInput", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)

		err := buf.Order(ids...)
		require.Nil(t, err, err)

		buf.Input() <- bs[0]
		close(buf.Input())

		for b := range buf.Output() { // check that still outputs block and closes Output
			assert.Equal(t, bs[0], b)
		}
	})

	t.Run("WithContext", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)
		err := buf.Order(ids...)
		require.Nil(t, err, err)

		// check that closing context terminates Buffer
		cancel()
		_, ok := <-buf.Output()
		assert.False(t, ok)
	})
}
