package blockstream

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/blocknet"
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

	reqs := make(chan *block.RequestGroup, 1)
	s := newTestRequester(t, ctx, reqs)

	// normal case
	req, err := block.NewRequestGroup(ctx, in[:8])
	require.NoError(t, err)

	reqs <- req
	assertBlockReq(t, s, 0, in)

	err = blocknet.writeBlocksResp(s, 0, bs, nil)
	require.Nil(t, err, err)

	out, err := req.Next()
	assert.Nil(t, err)
	assert.Equal(t, bs, out)

	out, err = req.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, out)

	// cancel case
	req, err = block.NewRequestGroup(ctx, in[8:16])
	require.NoError(t, err)

	reqs <- req
	req.Cancel()

	assertBlockReq(t, s, 1, in)
	assertBlockReq(t, s, 1, nil)

	err = blocknet.writeBlocksResp(s, 1, bs, nil)
	require.Nil(t, err, err)

	out, err = req.Next()
	assert.Nil(t, out)
	assert.Equal(t, io.EOF, err)

	// another normal case
	req, err = block.NewRequestGroup(ctx, in[16:24])
	require.NoError(t, err)

	reqs <- req
	assertBlockReq(t, s, 2, in)

	err = blocknet.writeBlocksResp(s, 2, bs, nil)
	require.Nil(t, err, err)

	out, err = req.Next()
	assert.Nil(t, err)
	assert.Equal(t, bs, out)

	out, err = req.Next()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, out)

	// error case
	req, err = block.NewRequestGroup(ctx, in[24:32])
	require.NoError(t, err)

	reqs <- req

	assertBlockReq(t, s, 3, in)

	err = blocknet.writeBlocksResp(s, 3, nil, blockstore.ErrNotFound)
	require.Nil(t, err, err)

	bs, err = req.Next()
	assert.Nil(t, bs)
	assert.Equal(t, blockstore.ErrNotFound, err)
}
