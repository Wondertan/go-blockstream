package blockstream

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestSessionStream(t *testing.T) {
	const (
		count   = 130
		size    = 64
		msgSize = 256
		tkn     = access.Token("test")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	get, remote := randBlockstore(t, rand.Reader, count/2, size)
	trk, local := randBlockstore(t, rand.Reader, count/2, size)

	ses, err := newSession(ctx, trk, nil, tkn, nil)
	require.Nil(t, err, err)

	ses.rcvrs = append(ses.rcvrs, rcv(t, ctx, tkn, get, trk, msgSize))
	ses.rcvrs = append(ses.rcvrs, rcv(t, ctx, tkn, get, trk, msgSize))
	ses.rcvrs = append(ses.rcvrs, rcv(t, ctx, tkn, get, trk, msgSize))

	in := make(chan []cid.Cid, 2)
	in <- append(remote, cid.Undef, cid.Undef)
	in <- local
	close(in)

	out := ses.Stream(ctx, in)
	assertChan(t, out, trk, count)
}

func TestSessionBlocks(t *testing.T) {
	const (
		count   = 130
		size    = 64
		msgSize = 256
		tkn     = access.Token("test")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	get, remote := randBlockstore(t, rand.Reader, count, size)
	trk, local := randBlockstore(t, rand.Reader, count, size)

	ses, err := newSession(ctx, trk, nil, tkn, nil)
	require.Nil(t, err, err)

	ses.rcvrs = append(ses.rcvrs, rcv(t, ctx, tkn, get, trk, msgSize))
	ses.rcvrs = append(ses.rcvrs, rcv(t, ctx, tkn, get, trk, msgSize))
	ses.rcvrs = append(ses.rcvrs, rcv(t, ctx, tkn, get, trk, msgSize))

	ch1, err := ses.Blocks(ctx, remote[:count/2])
	require.Nil(t, err, err)

	ch2, err := ses.Blocks(ctx, remote[count/2:])
	require.Nil(t, err, err)

	ch3, err := ses.Blocks(ctx, local)
	require.Nil(t, err, err)

	assertChan(t, ch1, trk, count/2)
	assertChan(t, ch2, trk, count/2)
	assertChan(t, ch3, trk, count)
}

func rcv(t *testing.T, ctx context.Context, tkn access.Token, get getter, put putter, max int) *receiver {
	eh := func(f func() error) {
		if err := f(); err != nil {
			t.Error(err)
		}
	}

	p, s := pair()
	go func() {
		_, err := newSender(s, get, max, func(token access.Token) error {
			return nil
		}, eh)
		require.Nil(t, err, err)
	}()

	r, err := newReceiver(ctx, put, p, tkn, eh)
	require.Nil(t, err, err)
	return r
}
