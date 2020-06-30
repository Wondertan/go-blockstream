package blockstream

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/test"
)

func TestRequester(t *testing.T) {
	const (
		count = 32
		size  = 32
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, in := test.RandBlocks(t, rand.Reader, count, size)

	reqs := make(chan *block.Request, 1)
	s := newTestRequester(t, ctx, reqs)

	req := block.NewRequest(ctx, 0, in)
	reqs <- req
	assertBlockReq(t, s, 0, in)

	err := writeBlocksResp(s, 0, bs)
	require.Nil(t, err, err)

	out, _ := req.Next()
	assert.Equal(t, bs, out)
}

func TestRequesterCancel(t *testing.T) {
	const (
		count = 32
		size  = 32
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqs := make(chan *block.Request, 1)
	s := newTestRequester(t, ctx, reqs)

	bs, in := test.RandBlocks(t, rand.Reader, count, size)

	req := block.NewRequest(ctx, 1, in)
	reqs <- req
	req.Cancel()

	assertBlockReq(t, s, 1, in)
	assertBlockReqCancel(t, s, 1)

	err := writeBlocksResp(s, 1, bs)
	require.Nil(t, err, err)

	bs, ok := req.Next()
	assert.Nil(t, bs)
	assert.False(t, ok)
}
