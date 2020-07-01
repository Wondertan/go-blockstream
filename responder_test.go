package blockstream

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/test"
)

func TestResponder(t *testing.T) {
	const (
		count = 10
		size  = 32
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := test.RandBlocks(t, rand.Reader, count, size)

	reqs := make(chan *block.Request, 8)
	s := newTestResponder(t, ctx, reqs)

	// normal case
	err := writeBlocksReq(s, uint32(1), ids[:4])
	require.Nil(t, err, err)

	req := <-reqs
	assert.Equal(t, uint32(1), req.Id())

	go func() {
		<-time.After(time.Millisecond)
		ok := req.Fill(bs[:2])
		assert.True(t, ok)
		ok = req.Fill(bs[2:4])
		assert.False(t, ok)
	}()

	assertBlockResp(t, s, 1, ids[:2], nil)
	assertBlockResp(t, s, 1, ids[2:4], nil)

	// cancel case
	err = writeBlocksReq(s, uint32(2), ids[4:8])
	require.Nil(t, err, err)

	err = writeBlocksReq(s, uint32(2), nil)
	require.Nil(t, err, err)

	req2 := <-reqs
	assert.Equal(t, uint32(2), req2.Id())

	_, err = req2.Next()
	assert.Equal(t, io.EOF, err)

	// another normal case
	err = writeBlocksReq(s, uint32(3), ids[8:])
	require.Nil(t, err, err)

	req3 := <-reqs
	assert.Equal(t, uint32(3), req3.Id())

	go func() {
		<-time.After(time.Millisecond)
		ok := req3.Fill(bs[8:9])
		assert.True(t, ok)
		ok = req3.Fill(bs[9:10])
		assert.False(t, ok)
	}()

	assertBlockResp(t, s, 3, ids[8:9], nil)
	assertBlockResp(t, s, 3, ids[9:10], nil)

	// error case
	err = writeBlocksReq(s, uint32(4), ids)
	require.Nil(t, err, err)

	req4 := <-reqs
	assert.Equal(t, uint32(4), req4.Id())

	req4.Error(blockstore.ErrNotFound)
	assertBlockResp(t, s, 4, nil, blockstore.ErrNotFound)
}
