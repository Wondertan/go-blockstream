package block

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/test"
)

func TestRequestInOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := test.RandBlocks(t, rand.Reader, 64, 256)
	req := newTestRequest(t, ctx, ids)

	out1 := req.RequestFor("out1")
	for i := len(bs) / 2; i >= 0; i-- {
		req.In(bs[i])
	}

	out2 := req.RequestFor("out2")
	for i := 0; i < len(bs)/2; i++ {
		b, ok := out1.Next()
		assert.True(t, ok)
		assert.True(t, b.Cid().Equals(ids[i]))

		b, ok = out2.Next()
		assert.True(t, ok)
		assert.True(t, b.Cid().Equals(ids[i]))
	}

	for i := len(bs) / 2; i < len(bs); i++ {
		req.In(bs[i])
		b, ok := out1.Next()
		assert.True(t, ok)
		assert.True(t, b.Cid().Equals(ids[i]))

		b, ok = out2.Next()
		assert.True(t, ok)
		assert.True(t, b.Cid().Equals(ids[i]))
	}

	_, ok := out1.Next()
	assert.False(t, ok)
	_, ok = out2.Next()
	assert.False(t, ok)

	out3 := req.RequestFor("out3")
	for i := 0; i < len(bs); i++ {
		b, ok := out3.Next()
		assert.True(t, ok)
		assert.True(t, b.Cid().Equals(ids[i]))
	}
}

func TestRequestError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs, ids := test.RandBlocks(t, rand.Reader, 8, 256)
	terr := fmt.Errorf("test error")

	req := newTestRequest(t, ctx, ids)
	for _, b := range bs[:len(bs)/2] {
		req.In(b)
	}
	time.Sleep(time.Millisecond) // TODO Fix this
	req.Error(terr)

	out, i := req.RequestFor("out"), 0
	for ; i < len(bs)/2; i++ {
		o, ok := out.Next()
		require.True(t, ok)
		assert.True(t, o.Cid().Equals(ids[i]))

		b, err := o.Get()
		require.NoError(t, err)
		assert.Equal(t, bs[i], b)
	}

	for ; i < len(bs); i++ {
		o, ok := out.Next()
		require.True(t, ok)
		assert.True(t, o.Cid().Equals(ids[i]))

		b, err := o.Get()
		require.Nil(t, b)
		assert.Equal(t, terr, err)
	}
}

func TestRequestID(t *testing.T) {
	_, ids1 := test.RandBlocks(t, rand.Reader, 8, 256)
	_, ids2 := test.RandBlocks(t, rand.Reader, 8, 256)

	id1, err := requestID(ids1)
	require.NoError(t, err)

	idSame, err := requestID(ids1)
	require.NoError(t, err)
	assert.True(t, id1.Equals(idSame))

	id2, err := requestID(ids2)
	require.NoError(t, err)
	assert.True(t, !id1.Equals(id2))

	idErr, err := requestID(nil)
	require.Error(t, err)
	assert.Empty(t, idErr)
}

func newTestRequest(t *testing.T, ctx context.Context, ids []cid.Cid) *RequestGroup {
	id, err := requestID(ids)
	require.NoError(t, err)
	return NewRequestGroup(ctx, id, ids)
}
