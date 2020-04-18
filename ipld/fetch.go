package ipld

import (
	"context"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

// FetchDAG traverses and fetches whole IPLD graph from the stream.
func FetchDAG(ctx context.Context, id cid.Cid, ses blockstream.BlockStreamer, dag format.NodeAdder) error {
	return Walk(ctx, id, ses, func(node format.Node) error {
		return dag.Add(ctx, node)
	})
}

// FetchAbsent traverses the IPLD graph and fetches nodes that are not in the Blockstore.
// TODO Blockstore is used because DAGService does not have a Has method.
func FetchAbsent(ctx context.Context, id cid.Cid, ses blockstream.BlockStreamer, bs blockstore.Blockstore) error {
	return Walk(ctx, id, ses, func(node format.Node) error {
		return bs.Put(node)
	}, Visit(func(id cid.Cid) (bool, error) {
		has, err := bs.Has(id)
		return !has, err
	}))
}
