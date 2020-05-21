package blockstream

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
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
		err := buf.Enqueue(ids...)
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
	const (
		count    = 64
		orders   = 8
		perOrder = count / orders
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlockstore(t, rand.Reader, count, 256)
	buf := NewBuffer(ctx, count, count)
	defer buf.Close()

	go func() {
		for i := 0; i < orders; i++ {
			ids := ids[i*perOrder : (i+1)*perOrder]

			err := buf.Enqueue(ids...) // define queue in new routine
			require.Nil(t, err, err)

			go func(ids []cid.Cid) {
				for i := len(ids) - 1; i >= 0; i-- {
					b, _ := bs.Get(ids[i])
					buf.Input() <- b // send blocks in reverse
				}
			}(ids)
		}
	}()

	// check requested queue
	for i := 0; i < len(ids)-1; i++ {
		select {
		case b := <-buf.Output():
			assert.Equal(t, ids[i], b.Cid())
		}
	}
	assert.Equal(t, buf.queue.Len(), uint32(0))
}

func TestBufferLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlocks(t, rand.Reader, 129, 256)
	buf := NewBuffer(ctx, 128, 128)

	err := buf.Enqueue(ids...)
	assert.Equal(t, errBufferOverflow, err)

	bs, ids = bs[:len(bs)-1], ids[:len(bs)-1]
	for _, b := range bs {
		buf.Input() <- b
	}

	assert.Equal(t, len(bs), buf.Len())
}

func TestBufferClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bs, ids := randBlocks(t, rand.Reader, 1, 256)

	t.Run("WithClose", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)
		err := buf.Enqueue(ids...)
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

		err := buf.Enqueue(ids...)
		require.Nil(t, err, err)

		buf.Input() <- bs[0]
		close(buf.Input())

		for b := range buf.Output() { // check that still outputs block and closes Output
			assert.Equal(t, bs[0], b)
		}
	})

	t.Run("WithContext", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)
		err := buf.Enqueue(ids...)
		require.Nil(t, err, err)

		// check that closing context terminates Buffer
		cancel()
		_, ok := <-buf.Output()
		assert.False(t, ok)
	})
}

func TestBufferCidList(t *testing.T) {
	buf := newList(256)
	_, ids := randBlocks(t, rand.Reader, 10, 256)

	in := ids[0]
	buf.Append(in)
	out := buf.Pop()
	assert.Equal(t, in, out)

	out = buf.Pop()
	assert.Equal(t, out, cid.Undef)

	// Check that link between items is not lost after popping.
	buf.Append(ids...)
	for _, id := range ids {
		out := buf.Pop()
		assert.Equal(t, id, out)
	}

	out = buf.Pop()
	assert.Equal(t, out, cid.Undef)
	assert.True(t, buf.Len() == 0)
}
