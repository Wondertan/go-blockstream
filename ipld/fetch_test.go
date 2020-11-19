package ipld

import (
	"context"
	"crypto/rand"
	"github.com/libp2p/go-libp2p-core/peer"
	"io"
	"testing"

	access "github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream"
)

func TestFetchAbsent(t *testing.T) {
	const (
		nsize = 512
		rsize = nsize * 256
	)

	ctx, cancel := context.WithCancel(access.WithToken(context.Background(), "test"))
	defer cancel()

	fillstore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))
	halfstore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))

	net, err := mocknet.FullMeshConnected(ctx, 2)
	require.Nil(t, err, err)
	hs := net.Hosts()

	r := blockstream.NewBlockStream(ctx, hs[0], fillstore, access.NewPassingGranter())
	l := blockstream.NewBlockStream(ctx, hs[1], halfstore, access.NewPassingGranter())

	lv1 := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)
	// copyBlockstore(t, ctx, fillstore, halfstore)
	lv2 := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)
	lv3 := dagFromReader(t, io.LimitReader(rand.Reader, rsize), &fakeAdder{fillstore}, nsize)

	n1 := merkledag.NodeWithData([]byte{1})
	n1.AddNodeLink("1", lv1)
	n1.AddNodeLink("2", lv2)
	n1.AddNodeLink("3", lv3)
	fillstore.Put(n1)

	n2 := merkledag.NodeWithData([]byte{2})
	n2.AddNodeLink("1", lv2)
	n2.AddNodeLink("2", lv2)
	n2.AddNodeLink("3", lv3)
	fillstore.Put(n2)

	n3 := merkledag.NodeWithData([]byte{3})
	n3.AddNodeLink("1", lv3)
	n3.AddNodeLink("2", lv2)
	n3.AddNodeLink("3", lv3)
	fillstore.Put(n3)

	root := merkledag.NodeWithData(nil)
	root.AddNodeLink("1", n1)
	root.AddNodeLink("2", n2)
	root.AddNodeLink("3", n3)
	fillstore.Put(root)

	ses, err := l.Session(ctx, []peer.ID{r.Host.ID()}, blockstream.Blockstore(halfstore), blockstream.Save(true))
	require.Nil(t, err, err)

	err = Traverse(context.Background(), root.Cid(), ses)
	require.Nil(t, err, err)

	assertEqualBlockstore(t, ctx, fillstore, halfstore)
	assertEqualBlockstore(t, ctx, halfstore, fillstore)
}
