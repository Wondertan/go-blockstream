package streaming

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-block-format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceiver(t *testing.T) {
	const (
		count = 32
		size  = 32
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tkn := Token("test")
	s1, s2 := pair()

	go checkHand(s2, func(token Token) error { return nil })
	rcv, err := newReceiver(ctx, s1, tkn, func(f func() error) {
		if err := f(); err != nil {
			t.Fatal(err)
		}
	})
	require.Nil(t, err, err)
	assert.NotNil(t, rcv.t)

	bs, in := randBlocks(t, rand.Reader, count, size)

	ch := make(chan blocks.Block)
	err = rcv.receive(ctx, in, ch)
	require.Nil(t, err, err)

	out, err := readBlocksReq(s2)
	require.Nil(t, err, err)
	assert.Equal(t, in, out)

	err = writeBlocksResp(s2, bs)
	require.Nil(t, err, err)

	for _, in := range bs {
		out := <-ch
		assert.Equal(t, in, out)
	}
}
