package block

import (
	"crypto/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"

	"github.com/Wondertan/go-blockstream/test"
)

func TestCidQueue(t *testing.T) {
	buf := newCidQueue()
	_, ids := test.RandBlocks(t, rand.Reader, 10, 256)

	in := ids[0]
	buf.Enqueue(in)
	out := buf.Dequeue()
	assert.Equal(t, in, out)

	out = buf.Dequeue()
	assert.Equal(t, out, cid.Undef)

	// Check that link between items is not lost after popping.
	buf.Enqueue(ids...)
	for _, id := range ids {
		out := buf.Dequeue()
		assert.Equal(t, id, out)
	}

	out = buf.Dequeue()
	assert.Equal(t, out, cid.Undef)
	assert.True(t, buf.Len() == 0)
}
