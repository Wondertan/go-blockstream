package blockstream

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/test"
)

func TestRequestResponder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := test.RandBlocks(t, rand.Reader, 8, 256)

	in, out := make(chan *block.Request, 4), make(chan *block.Request, 4)
	newRequestPair(ctx, in, out)

	reqIn := block.NewRequest(ctx, 0, ids)
	in <- reqIn

	reqOut := <-out
	for _, b := range bs {
		reqOut.Fill([]blocks.Block{b})
	}

	for _, b := range bs {
		bs, err := reqIn.Next()
		assert.Nil(t, err)
		assert.Equal(t, b, bs[0])
	}

	bs, err := reqIn.Next()
	assert.Nil(t, bs)
	assert.Equal(t, io.EOF, err)
}

func TestSessionStream(t *testing.T) {
	const (
		count   = 512
		times   = 8
		size    = 64
		msgSize = 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bstore, ids := test.RandBlockstore(t, rand.Reader, count, size)

	ses := newSession(ctx)
	addProvider(ctx, ses, bstore, msgSize)
	addProvider(ctx, ses, bstore, msgSize)
	addProvider(ctx, ses, bstore, msgSize)

	in := make(chan []cid.Cid, 2)
	go func() {
		for i := 0; i < times; i++ {
			in <- ids[i*count/times : (i+1)*count/times]
		}
		close(in)
	}()

	out := ses.Stream(ctx, in)
	for i := 0; i < times; i++ {
		assertChan(t, out, ids[i*count/times:(i+1)*count/times], count/times)
	}
}

func TestSessionBlocks(t *testing.T) {
	const (
		count   = 130
		size    = 64
		msgSize = 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bstore, ids := test.RandBlockstore(t, rand.Reader, count, size)

	ses := newSession(ctx)
	addProvider(ctx, ses, bstore, msgSize)
	addProvider(ctx, ses, bstore, msgSize)
	addProvider(ctx, ses, bstore, msgSize)

	ch1, err := ses.Blocks(ctx, ids[:count/2])
	require.Nil(t, err, err)

	ch2, err := ses.Blocks(ctx, ids[count/2:])
	require.Nil(t, err, err)

	assertChan(t, ch1, ids[:count/2], count/2)
	assertChan(t, ch2, ids[count/2:], count/2)
}

func TestSessionNotFound(t *testing.T) {
	const (
		count   = 10
		size    = 64
		msgSize = 256
		missing = 5
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bstore, ids := test.RandBlockstore(t, rand.Reader, count, size)
	bstore.DeleteBlock(ids[missing])

	ses := newSession(ctx)
	addProvider(ctx, ses, bstore, msgSize)
	addProvider(ctx, ses, bstore, msgSize)
	addProvider(ctx, ses, bstore, msgSize)

	ch, err := ses.Blocks(ctx, ids)
	require.Nil(t, err, err)

	for range make([]bool, missing) {
		_, ok := <-ch
		assert.True(t, ok)
	}

	_, ok := <-ch
	assert.False(t, ok)
}

func addProvider(ctx context.Context, ses *Session, bstore blockstore.Blockstore, msgSize int) {
	reqs := make(chan *block.Request, 8)
	s1, s2 := streamPair()
	newResponder(ctx, s2, reqs, logClose)
	block.NewCollector(ctx, reqs, bstore, msgSize)
	ses.addProvider(s1, logClose)
}
