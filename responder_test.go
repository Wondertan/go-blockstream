package blockstream

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := randBlocks(t, rand.Reader, 10, 256)

	reqs := make(chan *request, 8)
	s := newTestResponder(t, ctx, reqs)

	err := writeBlocksReq(s, uint32(1), ids[:4])
	require.Nil(t, err, err)

	req := <-reqs
	assert.Equal(t, uint32(1), req.id)
	assert.Equal(t, ids[:4], req.ids)

	go func() {
		<-time.After(time.Millisecond)
		ok := req.Fill(bs[:2])
		assert.True(t, ok)
		ok = req.Fill(bs[2:4])
		assert.False(t, ok)
	}()

	assertBlockResp(t, s, 1, ids[:2])
	assertBlockResp(t, s, 1, ids[2:4])

	err = writeBlocksReq(s, uint32(2), ids[4:8])
	require.Nil(t, err, err)

	err = writeBlocksReq(s, uint32(2), nil)
	require.Nil(t, err, err)

	req2 := <-reqs
	assert.Equal(t, uint32(2), req2.id)
	assert.Equal(t, ids[4:8], req2.ids)

	select {
	case <-req2.Done():
	default:
		t.Error("not closed")
	}

	err = writeBlocksReq(s, uint32(3), ids[8:])
	require.Nil(t, err, err)

	req3 := <-reqs
	assert.Equal(t, uint32(3), req3.id)
	assert.Equal(t, ids[8:], req3.ids)

	go func() {
		<-time.After(time.Millisecond)
		ok := req3.Fill(bs[8:9])
		assert.True(t, ok)
		ok = req3.Fill(bs[9:10])
		assert.False(t, ok)

	}()

	assertBlockResp(t, s, 3, ids[8:9])
	assertBlockResp(t, s, 3, ids[9:10])
}
