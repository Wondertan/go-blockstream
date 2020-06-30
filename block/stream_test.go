package block

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/test"
)

func TestBlockStreamOrder(t *testing.T) {
	const (
		count    = 256
		orders   = 32
		perOrder = count / orders
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bstore, ids := test.RandBlockstore(t, rand.Reader, count, 256)
	buf := NewStream(ctx, NewSimpleCache(), count)
	defer buf.Close()

	go func() {
		for i := 0; i < orders; i++ {
			err := buf.Enqueue(ids[i*perOrder : (i+1)*perOrder]...)
			require.Nil(t, err, err)
		}

		for i := orders - 1; i >= 0; i-- {
			bs := make([]blocks.Block, 0, perOrder)
			for _, id := range ids[i*perOrder : (i+1)*perOrder] {
				b, _ := bstore.Get(id)
				bs = append(bs, b)
			}

			go func(bs []blocks.Block) {
				buf.Input() <- bs
			}(bs)
		}
	}()

	// check requested queue
	for i := 0; i < len(ids)-1; i++ {
		b := <-buf.Output()
		assert.Equal(t, ids[i], b.Cid())
	}
	assert.Equal(t, buf.queue.Len(), uint32(0))
}

func TestBlockStreamUnbounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewStream(ctx, NewSimpleCache(), 256)
	defer buf.Close()

	bs, _ := test.RandBlocks(t, rand.Reader, 256, 256)

	tmr := time.NewTimer(10 * time.Millisecond)
	defer tmr.Stop()

	// Check that stream is unbounded and blocks can be written without deadlocking.
	for i := 0; i < 16; i++ {
		select {
		case buf.Input() <- bs[i*16 : (i+1)*16]:
			tmr.Reset(10 * time.Millisecond)
		case <-tmr.C:
			t.Fatal("Buffer input is blocked.")
		}
	}
}

func TestBlockStreamClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bs, ids := test.RandBlocks(t, rand.Reader, 1, 256)

	t.Run("WithClose", func(t *testing.T) {
		buf := NewStream(ctx, NewSimpleCache(), 32)
		err := buf.Enqueue(ids...)
		require.Nil(t, err, err)

		err = buf.Close()
		require.Nil(t, err, err)

		buf.Input() <- bs
		for b := range buf.Output() { // check that still outputs block and closes Output
			assert.Equal(t, bs[0], b)
		}
	})

	t.Run("WithInput", func(t *testing.T) {
		buf := NewStream(ctx, NewSimpleCache(), 32)

		err := buf.Enqueue(ids...)
		require.Nil(t, err, err)

		buf.Input() <- bs
		close(buf.Input())

		for b := range buf.Output() { // check that still outputs block and closes Output
			assert.Equal(t, bs[0], b)
		}
	})

	t.Run("WithContext", func(t *testing.T) {
		buf := NewStream(ctx, NewSimpleCache(), 32)
		err := buf.Enqueue(ids...)
		require.Nil(t, err, err)

		// check that closing context terminates Buffer
		cancel()
		_, ok := <-buf.Output()
		assert.False(t, ok)
	})

	t.Run("Closed", func(t *testing.T) {
		buf := NewStream(ctx, NewSimpleCache(), 32)
		err := buf.Close()
		require.Nil(t, err, err)

		err = buf.Close()
		assert.Equal(t, errStreamClosed, err)

		err = buf.Enqueue(ids...)
		assert.Equal(t, errStreamClosed, err)
	})
}
