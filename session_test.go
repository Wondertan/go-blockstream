package blockstream

import (
	"context"
	"crypto/rand"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestResponder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlocks(t, rand.Reader, 8, 256)

	in, out := make(chan *request, 4), make(chan *request, 4)
	newRequestPair(ctx, in, out)

	reqIn := newRequest(ctx, 0, ids)
	in <- reqIn

	reqOut := <-out
	for _, b := range bs {
		reqOut.Fill([]blocks.Block{b})
	}

	for _, b := range bs {
		bs, _ := reqIn.Next()
		assert.Equal(t, b, bs[0])
	}
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

	bstore, ids := randBlockstore(t, rand.Reader, count, size)

	ses := newSession(ctx, &fakeTracker{})
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

	bstore, ids := randBlockstore(t, rand.Reader, count, size)

	ses := newSession(ctx, &fakeTracker{})
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

func addProvider(ctx context.Context, ses *Session, bstore blockstore.Blockstore, msgSize int) {
	reqs := make(chan *request, 8)
	s1, s2 := streamPair()
	newResponder(ctx, s2, reqs, closeLog)
	newCollector(ctx, reqs, bstore, msgSize, closeLog)
	ses.addProvider(s1, closeLog)
}
