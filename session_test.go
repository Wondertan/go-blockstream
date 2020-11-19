package blockstream

import (
	"context"
	"crypto/rand"
	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/test"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

func TestRequestResponder(t *testing.T) {
	const (
		count   = 8
		size    = 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := test.RandBlocks(t, rand.Reader, count, size)

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
		assert.NoError(t, err, err)
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

	res, err := ses.Stream(ctx, in)
	for i := 0; i < times; i++ {
		for _, id := range ids[i*count/times:(i+1)*count/times] {
			res, ok := <-res
			require.True(t, ok)
			assert.Equal(t, id, res.Cid)
			assert.NotNil(t, res.Block)
			assert.NoError(t, res.Error)
		}
	}

	_, ok := <-err
	assert.False(t, ok)
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

	res1, err1 := ses.Blocks(ctx, ids[:count/2])
	res2, err2 := ses.Blocks(ctx, ids[count/2:])

	for _, id := range ids[:count/2] {
		res, ok := <-res1
		require.True(t, ok)
		assert.Equal(t, id, res.Cid)
		assert.NotNil(t, res.Block)
		assert.NoError(t, res.Error)
	}

	_, ok := <-err1
	assert.False(t, ok)

	for _, id := range ids[count/2:] {
		res, ok := <-res2
		require.True(t, ok)
		assert.Equal(t, id, res.Cid)
		assert.NotNil(t, res.Block)
		assert.NoError(t, res.Error)
	}

	_, ok = <-err2
	assert.False(t, ok)
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

	var i int
	res, err := ses.Blocks(ctx, ids)
	for res := range res {
		if i == missing {
			assert.Error(t, res.Error)
		}
		i++
	}

	_, ok := <-err
	assert.False(t, ok)
}

func TestSessionBlockstoreSave(t *testing.T) {
	const (
		count   = 32
		size    = 64
		msgSize = 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	empty := test.EmptyBlockstore()
	bstore, ids := test.RandBlockstore(t, rand.Reader, count, size)

	ses := newSession(ctx, Blockstore(empty), Save(true))
	addProvider(ctx, ses, bstore, msgSize)

	_, err := ses.Blocks(ctx, ids)
	_, ok := <-err
	assert.False(t, ok)

	time.Sleep(time.Millisecond * 100) // Needs some time to save
	for _, id := range ids {
		ok, err := empty.Has(id)
		require.NoError(t, err, err)
		assert.True(t, ok, "Some blocks are missing")
	}
}

func addProvider(ctx context.Context, ses *Session, bstore blockstore.Blockstore, msgSize int) {
	reqs := make(chan *block.Request, 8)
	s1, s2 := streamPair()
	newResponder(ctx, s2, reqs, logClose)
	block.NewCollector(ctx, reqs, bstore, msgSize)
	ses.addProvider(s1, logClose)
}
