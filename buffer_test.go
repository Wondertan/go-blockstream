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

	buf := newBuffer(ctx, 8, 256)
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

	buf := newBuffer(ctx, 32, 32)
	bs, ids := randBlocks(t, rand.Reader, 32, 256)
	err := buf.Order(ids[:16]...)
	require.Nil(t, err, err)

	go func() {
		err := buf.Order(ids[16:]...)
		require.Nil(t, err, err)

		err = buf.Close()
		require.Nil(t, err, err)
	}()

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
	assert.Equal(t, len(bs), i)
}

func TestBufferClosing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	bs, ids := randBlocks(t, rand.Reader, 1, 256)

	buf := newBuffer(ctx, 32, 32)
	err := buf.Order(ids...)
	require.Nil(t, err, err)

	err = buf.Close()
	require.Nil(t, err, err)

	buf.Input() <- bs[0]
	for b := range buf.Output() { // check that outputs one block and is closed after Order is closed
		assert.Equal(t, bs[0], b)
	}

	buf = newBuffer(ctx, 32, 32)
	err = buf.Order(ids...)
	require.Nil(t, err, err)

	// check that closing context terminates Buffer
	buf.Input() <- bs[0]
	cancel()
	_, ok := <-buf.Output()
	assert.False(t, ok)
}
