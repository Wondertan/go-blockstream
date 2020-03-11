package streaming

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSession(t *testing.T) {
	const (
		count   = 129
		size    = 64
		msgSize = 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tkn := Token("test")
	bs, ids := randBlockstore(t, rand.Reader, count, size)

	ses, err := newSession(ctx, nil, tkn, nil)
	require.Nil(t, err, err)

	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))
	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))
	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))

	ch1, err := ses.GetBlocks(ctx, ids[:count/2])
	require.Nil(t, err, err)

	ch2, err := ses.GetBlocks(ctx, ids[count/2:])
	require.Nil(t, err, err)

	var hits int
	check(t, ch1, bs, &hits)
	check(t, ch2, bs, &hits)
	assert.Equal(t, count, hits)
}

func check(t *testing.T, ch <-chan blocks.Block, bs blockstore.Blockstore, hits *int) {
	for b := range ch {
		ok, err := bs.Has(b.Cid())
		assert.Nil(t, err, err)
		assert.True(t, ok)
		*hits++
	}
}

func rcv(t *testing.T, ctx context.Context, tkn Token, blocks getter, max int) *receiver {
	eh := func(f func() error) {
		if err := f(); err != nil {
			t.Error(err)
		}
	}

	p, s := pair()
	go func() {
		_, err := newSender(s, blocks, max, func(token Token) error {
			return nil
		}, eh)
		require.Nil(t, err, err)
	}()

	r, err := newReceiver(ctx, p, tkn, eh)
	require.Nil(t, err, err)
	return r
}
