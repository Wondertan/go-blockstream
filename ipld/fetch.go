package ipld

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

func FetchDAG(ctx context.Context, id cid.Cid, ses blockstream.BlockStreamer, dag format.NodeAdder) error {
	return Walk(ctx, id, ses, VisitAll(func(node format.Node) error {
		return dag.Add(ctx, node)
	}))
}
