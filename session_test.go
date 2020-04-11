package blockstream

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestSessionStream(t *testing.T) {
	const (
		count   = 130
		size    = 64
		msgSize = 256
		tkn     = Token("test")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlockstore(t, rand.Reader, count, size)

	ses, err := newSession(ctx, &nilPutter{}, nil, tkn, nil)
	require.Nil(t, err, err)

	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))
	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))
	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))

	in := make(chan []cid.Cid, 1)
	in <- ids
	close(in)

	out := ses.Stream(ctx, in)
	assertChan(t, out, bs, count)
}

func TestSessionBlocks(t *testing.T) {
	const (
		count   = 130
		size    = 64
		msgSize = 256
		tkn     = Token("test")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlockstore(t, rand.Reader, count, size)

	ses, err := newSession(ctx, &nilPutter{}, nil, tkn, nil)
	require.Nil(t, err, err)

	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))
	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))
	ses.addReceiver(rcv(t, ctx, tkn, bs, msgSize))

	ch1 := ses.Blocks(ctx, ids[:count/2])
	ch2 := ses.Blocks(ctx, ids[count/2:])

	assertChan(t, ch1, bs, count/2)
	assertChan(t, ch2, bs, count/2)
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

	r, err := newReceiver(ctx, &nilPutter{}, p, tkn, eh)
	require.Nil(t, err, err)
	return r
}
