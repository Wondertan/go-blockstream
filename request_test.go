package blockstream

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ids := randBlocks(t, rand.Reader, 8, 256)
	rq := newRequestQueue(ctx.Done())

	rq.Enqueue(newRequest(ctx, 0, ids))
	rq.Enqueue(newRequest(ctx, 1, ids))
	rq.Enqueue(newRequest(ctx, 2, ids))
	rq.Enqueue(newRequest(ctx, 3, ids))

	rq.Cancel(1)
	rq.Cancel(2)

	req := rq.Back()
	assert.Equal(t, uint32(0), req.Id())
	rq.PopBack()

	req = rq.Back()
	assert.Equal(t, uint32(3), req.Id())
	rq.PopBack()

	assert.True(t, rq.Len() == 0, rq.Len())
}
