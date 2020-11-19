package blockstream

import (
	"context"
	"crypto/rand"
	"github.com/Wondertan/go-blockstream/block"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-libp2p-access"

	"github.com/Wondertan/go-blockstream/test"
)

func TestBlockStream(t *testing.T) {
	const (
		nodesCount  = 5
		blocksCount = 256
		size        = 64
		tkn         = access.Token("test")
	)

	ctx := access.WithToken(context.Background(), tkn)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bs, cids := test.RandBlockstore(t, rand.Reader, blocksCount, size)

	net, err := mocknet.FullMeshConnected(ctx, nodesCount)
	require.NoError(t, err, err)
	hs := net.Hosts()

	nodes := make([]*BlockStream, nodesCount)
	for i, h := range hs {
		nodes[i] = NewBlockStream(ctx, h, bs, access.NewGranter())
	}

	wg := new(sync.WaitGroup)
	once := new(sync.Once)

	ctx, cancel = context.WithCancel(ctx)
	sessions := make([]*Session, nodesCount)
	errs := make([]<-chan error, nodesCount)
	for i, n := range nodes {
		peers := make([]peer.ID, 0, nodesCount-1)
		for _, h := range hs {
			if h == n.Host {
				continue
			}
			peers = append(peers, h.ID())
		}

		errs[i] = n.Granter.Grant(context.Background(), tkn, peers...)

		wg.Add(1)
		go func(i int, n *BlockStream) {
			defer wg.Done()

			var er error
			sessions[i], er = n.Session(ctx, peers)
			if er != nil {
				once.Do(func() {
					err = er
				})
			}
		}(i, n)
	}

	wg.Wait()
	require.Nil(t, err, err)

	results, errs := make([]<-chan block.Result, nodesCount), make([]<-chan error, nodesCount)
	for i, s := range sessions {
		results[i], errs[i] = s.Blocks(ctx, cids)
	}

	for i, ch := range results {
		for _, id := range cids {
			res, ok := <-ch
			require.True(t, ok)
			assert.Equal(t, id, res.Cid)
			assert.NotNil(t, res.Block)
			assert.NoError(t, res.Error)
		}

		_, ok := <-errs[i]
		assert.False(t, ok)
	}

	cancel()
	for _, ch := range errs {
		for err := range ch {
			t.Error(err)
		}
	}

	for _, n := range nodes {
		err = n.Close()
		require.NoError(t, err, err)
	}
}
