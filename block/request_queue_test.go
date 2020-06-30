package block

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Wondertan/go-blockstream/test"
)

func TestRequestQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, ids := test.RandBlocks(t, rand.Reader, 8, 256)
	rq := NewRequestQueue(ctx.Done())

	rq.Enqueue(NewRequest(ctx, 0, ids))
	rq.Enqueue(NewRequest(ctx, 1, ids))
	rq.Enqueue(NewRequest(ctx, 2, ids))
	rq.Enqueue(NewRequest(ctx, 3, ids))

	rq.Cancel(1)
	rq.Cancel(2)

	req := rq.BackPopDone()
	assert.Equal(t, uint32(0), req.Id())
	rq.PopBack()

	req = rq.BackPopDone()
	assert.Equal(t, uint32(3), req.Id())
	rq.PopBack()

	assert.True(t, rq.Len() == 0, rq.Len())
}
