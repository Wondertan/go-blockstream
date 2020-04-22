package ipld

import (
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchDAG(t *testing.T) {
	const (
		nsize = 512
		rsize = nsize * 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fillstore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))
	emptystore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))

	nd := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)
	err := FetchDAG(ctx, nd.Cid(), &offlineStreamer{getter: fillstore.Get}, &fakeAdder{emptystore})
	require.Nil(t, err, err)

	assertEqualBlockstore(t, ctx, fillstore, emptystore)
	assertEqualBlockstore(t, ctx, emptystore, fillstore)
}

func TestFetchAbsent(t *testing.T) {
	const (
		nsize = 512
		rsize = nsize * 256
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fillstore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))
	halfstore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))

	lv1 := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)
	copyBlockstore(t, ctx, fillstore, halfstore)
	lv2 := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)
	lv3 := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)

	n1 := merkledag.NodeWithData([]byte{1})
	n1.AddNodeLink("1", lv1)
	n1.AddNodeLink("2", lv2)
	n1.AddNodeLink("3", lv3)
	fillstore.Put(n1)

	n2 := merkledag.NodeWithData([]byte{2})
	n2.AddNodeLink("1", lv1)
	n2.AddNodeLink("2", lv2)
	n2.AddNodeLink("3", lv3)
	fillstore.Put(n2)

	n3 := merkledag.NodeWithData([]byte{3})
	n3.AddNodeLink("1", lv1)
	n3.AddNodeLink("2", lv2)
	n3.AddNodeLink("3", lv3)
	fillstore.Put(n3)

	root := merkledag.NodeWithData(nil)
	root.AddNodeLink("1", n1)
	root.AddNodeLink("2", n2)
	root.AddNodeLink("3", n3)
	fillstore.Put(root)

	s := &offlineStreamer{getter: fillstore.Get}
	err := FetchAbsent(ctx, root.Cid(), s, halfstore)
	require.Nil(t, err, err)

	assertEqualBlockstore(t, ctx, fillstore, halfstore)
	assertEqualBlockstore(t, ctx, halfstore, fillstore)

	for _, id := range s.streamed {
		if id.Equals(lv1.Cid()) {
			t.Fatal("streamed existed blocks")
		}
	}
}

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

func (f *offlineStreamer) Stream(ctx context.Context, ids <-chan []cid.Cid) <-chan blocks.Block {
	out := make(chan blocks.Block)
	go func() {
		defer close(out)
		for {
			select {
			case ids, ok := <-ids:
				if !ok {
					return
				}

				for _, id := range ids {
					b, err := f.getter(id)
					if err != nil {
						return
					}
					f.streamed = append(f.streamed, id)

					select {
					case out <- b:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
