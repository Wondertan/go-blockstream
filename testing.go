package blockstream

import (
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/blocknet"
)

func assertChan(t *testing.T, ch <-chan blocks.Block, ids []cid.Cid, expected int) {
	var actual int
	for _, id := range ids {
		b := <-ch
		assert.Equal(t, id, b.Cid())
		actual++
	}
	assert.Equal(t, expected, actual)
}

func assertBlockReq(t *testing.T, r io.Reader, in uint32, ids []cid.Cid) {
	id, out, err := blocknet.readBlocksReq(r)
	require.Nil(t, err, err)
	assert.Equal(t, ids, out)
	assert.Equal(t, in, id)
}

func assertBlockResp(t *testing.T, r io.Reader, in uint32, ids []cid.Cid, errIn error) {
	id, out, errOut, err := blocknet.readBlocksResp(r)
	require.Nil(t, err, err)
	assert.Equal(t, in, id)
	assert.Equal(t, len(ids), len(out))
	assert.Equal(t, errIn, errOut)
	for i, b := range out {
		_, err = blocknet.newBlockCheckCid(b, ids[i])
		require.Nil(t, err, err)
	}
}

func newRequestPair(ctx context.Context, in, out chan *block.RequestGroup) {
	s1, s2 := streamPair()
	newRequester(ctx, s1, in, logClose)
	newResponder(ctx, s2, out, logClose)
}

func newTestResponder(t *testing.T, ctx context.Context, reqs chan *block.RequestGroup) io.ReadWriter {
	s1, s2 := streamPair()
	newResponder(ctx, s2, reqs, logClose)
	return s1
}

func newTestRequester(t *testing.T, ctx context.Context, reqs chan *block.RequestGroup) io.ReadWriter {
	s1, s2 := streamPair()
	newRequester(ctx, s2, reqs, logClose)
	return s1
}


