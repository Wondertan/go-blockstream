package blockstream

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
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
	for i := 0; i < 16; i++ {
		select {
		case buf.Input() <- bs[i*16 : (i+1)*16]:
			tmr.Reset(10 * time.Millisecond)
		case <-tmr.C:
			t.Fatal("Buffer input is blocked.")
		}
	}
}

func TestBufferOrder(t *testing.T) {
	const (
		count    = 256
		orders   = 32
		perOrder = count / orders
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bstore, ids := randBlockstore(t, rand.Reader, count, 256)
	buf := NewBuffer(ctx, count, count)
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

func TestBufferLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, _ := randBlocks(t, rand.Reader, 128, 256)
	buf := NewBuffer(ctx, 128, 128)

	buf.Input() <- bs
	time.Sleep(100 * time.Microsecond) // fixes flakyness
	assert.Equal(t, len(bs), buf.Len())
}

// func TestBufferLimit(t *testing.T) {
// 	const limit = 128
//
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
//
// 	bs, ids := randBlocks(t, rand.Reader, 256, 256)
// 	buf := NewBuffer(ctx, 64, limit)
//
// 	err := buf.Enqueue(ids...)
// 	assert.Equal(t, errBufferOverflow, err)
//
// 	err = buf.Enqueue(ids[:len(ids)/2]...)
// 	require.Nil(t, err, err)
//
// 	tmr := time.NewTimer(10 * time.Millisecond)
// 	defer tmr.Stop()
//
// 	for i, b := range bs { // check that buffer will not grow more than a limit
// 		select {
// 		case buf.Input() <- b:
// 			tmr.Reset(10 * time.Millisecond)
// 			if i != len(bs)-1 {
// 				continue
// 			}
// 		case <-tmr.C:
// 		}
//
// 		assert.Equal(t, limit, i)
// 		break
// 	}
//
// 	for i := 0; i < limit; i++ { // check that after blocking it is possible to read the Blocks
// 		b := <-buf.Output()
// 		assert.Equal(t, ids[i], b.Cid())
// 	}
// }

func TestBufferClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bs, ids := randBlocks(t, rand.Reader, 1, 256)

	t.Run("WithClose", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)
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
		buf := NewBuffer(ctx, 32, 32)

		err := buf.Enqueue(ids...)
		require.Nil(t, err, err)

		buf.Input() <- bs
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

	t.Run("Closed", func(t *testing.T) {
		buf := NewBuffer(ctx, 32, 32)
		err := buf.Close()
		require.Nil(t, err, err)

		err = buf.Close()
		assert.Equal(t, errBufferClosed, err)

		err = buf.Enqueue(ids...)
		assert.Equal(t, errBufferClosed, err)
	})
}

func TestBufferCidList(t *testing.T) {
	buf := newCidQueue(256)
	_, ids := randBlocks(t, rand.Reader, 10, 256)

	in := ids[0]
	buf.Enqueue(in)
	out := buf.Dequeue()
	assert.Equal(t, in, out)

	out = buf.Dequeue()
	assert.Equal(t, out, cid.Undef)

	// Check that link between items is not lost after popping.
	buf.Enqueue(ids...)
	for _, id := range ids {
		out := buf.Dequeue()
		assert.Equal(t, id, out)
	}

	out = buf.Dequeue()
	assert.Equal(t, out, cid.Undef)
	assert.True(t, buf.Len() == 0)
}
