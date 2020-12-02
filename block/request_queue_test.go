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

	req1 := newTestRequest(t, ctx, ids[:2])
	rq.Enqueue(req1)

	req2 := newTestRequest(t, ctx, ids[2:4])
	rq.Enqueue(req2)

	req3 := newTestRequest(t, ctx, ids[4:6])
	rq.Enqueue(req3)

	req4 := newTestRequest(t, ctx, ids[6:8])
	rq.Enqueue(req4)

	rq.Cancel(req1.Id())
	rq.Cancel(req2.Id())

	req := rq.Back()
	assert.True(t, req1.Id().Equals(req.Id()))
	rq.PopBack()

	req = rq.BackPopDone()
	assert.True(t, req3.Id().Equals(req.Id()))
	rq.PopBack()

	req = rq.BackPopDone()
	assert.True(t, req4.Id().Equals(req.Id()))
	rq.PopBack()

	assert.True(t, rq.Len() == 0, rq.Len())
}
