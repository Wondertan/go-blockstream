package ipld

import (
	"context"
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertEqualBlockstore(t *testing.T, ctx context.Context, exp, given blockstore.Blockstore) {
	ids, err := exp.AllKeysChan(ctx)
	require.Nil(t, err, err)

	for id := range ids {
		ok, err := given.Has(id)
		require.Nil(t, err, err)
		assert.True(t, ok, "store does not have required block")
	}
}

func copyBlockstore(t *testing.T, ctx context.Context, from, to blockstore.Blockstore) {
	ch, err := from.AllKeysChan(ctx)
	if err != nil {
		t.Fatal(err)
	}

	for id := range ch {
		b, err := from.Get(id)
		if err != nil {
			t.Fatal(err)
		}

		err = to.Put(b)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func dagFromReader(t *testing.T, rand io.Reader, bs format.NodeAdder, nsize int) format.Node {
	var nodes []*merkledag.ProtoNode
	for {
		buf := make([]byte, nsize)
		_, err := io.ReadFull(rand, buf)
		if err == io.EOF {
			break
		}
		require.Nil(t, err, err)

		nodes = append(nodes, merkledag.NodeWithData(buf))
	}

	root := merkledag.NodeWithData(nil)
	for _, n := range nodes {
		err := root.AddNodeLink(n.Cid().String(), n)
		require.Nil(t, err, err)

		err = bs.Add(context.TODO(), n)
		require.Nil(t, err, err)
	}

	err := bs.Add(context.TODO(), root)
	require.Nil(t, err, err)

	return root
}

type fakeAdder struct {
	bs blockstore.Blockstore
}

func (f *fakeAdder) Add(_ context.Context, nd format.Node) error {
	return f.bs.Put(nd)
}

func (f *fakeAdder) AddMany(_ context.Context, nds []format.Node) error {
	bs := make([]blocks.Block, len(nds))
	for i, nd := range nds {
		bs[i] = nd
	}
	return f.bs.PutMany(bs)
}

type offlineStreamer struct {
	getter   func(cid.Cid) (blocks.Block, error)
	streamed []cid.Cid
}

func (f *offlineStreamer) Stream(ctx context.Context, ids <-chan []cid.Cid) (<-chan blocks.Block, <-chan error) {
	out := make(chan blocks.Block, 500)

	cherr := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(cherr)

		for {
			select {
			case ids, ok := <-ids:
				if !ok {
					return
				}

				for _, id := range ids {
					b, err := f.getter(id)
					if err != nil {
						cherr <- err
						return
					}
					f.streamed = append(f.streamed, id)

					select {
					case out <- b:
					case <-ctx.Done():
						cherr <- ctx.Err()
						return
					}
				}
			case <-ctx.Done():
				cherr <- ctx.Err()
				return
			}
		}
	}()

	return out, cherr
}
