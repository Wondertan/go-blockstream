package blockstream

import (
	"context"
	"io"
	"testing"

	access "github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func MockNet(t *testing.T, ctx context.Context, count int) []*BlockStream {
	net, err := mocknet.FullMeshConnected(ctx, count)
	require.Nil(t, err, err)
	hs := net.Hosts()

	nodes := make([]*BlockStream, count)
	for i, h := range hs {
		nodes[i] = NewBlockStream(h, nil, access.NewPassingGranter())
	}

	return nodes
}

func randBlockstore(t *testing.T, rand io.Reader, count, size int) (blockstore.Blockstore, []cid.Cid) {
	bstore := newBlockstore()
	bs, ids := randBlocks(t, rand, count, size)
	for _, b := range bs {
		err := bstore.Put(b)
		if err != nil {
			t.Fatal(err)
		}
	}

	return bstore, ids
}

func randBlocks(t *testing.T, rand io.Reader, count, size int) ([]blocks.Block, []cid.Cid) {
	bs := make([]blocks.Block, count)
	ids := make([]cid.Cid, count)
	for i := 0; i < count; i++ {
		b := make([]byte, size)
		_, err := rand.Read(b)
		if err != nil {
			t.Fatal(err)
		}

		bs[i] = blocks.NewBlock(b)
		ids[i] = bs[i].Cid()
	}

	return bs, ids
}

func assertChan(t *testing.T, ch <-chan blocks.Block, bs blockstore.Blockstore, expected int) {
	var actual int
	for b := range ch {
		ok, err := bs.Has(b.Cid())
		assert.Nil(t, err, err)
		assert.True(t, ok)
		actual++
	}
	assert.Equal(t, expected, actual)
}

type fakeStream struct {
	read  *io.PipeReader
	write *io.PipeWriter
}

func pair() (*fakeStream, *fakeStream) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return &fakeStream{r1, w2}, &fakeStream{r2, w1}
}

func (s *fakeStream) Read(b []byte) (n int, err error) {
	return s.read.Read(b)
}

func (s *fakeStream) Write(b []byte) (n int, err error) {
	return s.write.Write(b)
}

func (s *fakeStream) Close() error {
	return s.write.Close()
}

type fakeTracker struct{}

func (t fakeTracker) Has(cid.Cid) (bool, error) { return false, nil }

func (fakeTracker) PutMany([]blocks.Block) error { return nil }

func (fakeTracker) GetSize(cid.Cid) (int, error) { return 0, nil }

func (fakeTracker) Get(cid.Cid) (blocks.Block, error) { return nil, blockstore.ErrNotFound }
