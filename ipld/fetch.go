package ipld

import (
	"context"
	blocks "github.com/ipfs/go-block-format"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

// FetchDAG traverses and fetches whole IPLD graph from the stream.
func FetchDAG(ctx context.Context, id cid.Cid, ses blockstream.Streamer, dag format.NodeAdder) error {
	return blockstream.Explore(ctx, id, ses, func(b blocks.Block) ([]cid.Cid, error) {
		nd, err := format.Decode(b)
		if err != nil {
			return nil, err
		}

		ids := make([]cid.Cid, len(nd.Links()))
		for i, l := range nd.Links() {
			ids[i] = l.Cid
		}

		return ids, dag.Add(ctx, nd)
	})
}

// FetchAbsent traverses the IPLD graph and fetches nodes that are not in the Blockstore.
// TODO Blockstore is used because DAGService does not have a Has method.
func FetchAbsent(ctx context.Context, id cid.Cid, ses blockstream.Streamer, bs blockstore.Blockstore) error {
	return blockstream.Explore(ctx, id, ses, func(b blocks.Block) ([]cid.Cid, error) {
		nd, err := format.Decode(b)
		if err != nil {
			return nil, err
		}

		ids := make([]cid.Cid, 0, len(nd.Links()))
		for _, l := range nd.Links() {
			ok, err := bs.Has(l.Cid)
			if err != nil {
				return nil, err
			}

			if !ok {
				ids = append(ids, l.Cid)
			}

		}

		return ids, bs.Put(nd)
	})
}
