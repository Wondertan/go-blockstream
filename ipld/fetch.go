package ipld

import (
	"context"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

// Traverse traverses and fetches whole IPLD graph from the stream.
func Traverse(ctx context.Context, id cid.Cid, ses blockstream.Streamer) error {
	return blockstream.Explore(ctx, id, ses, func(res block.Result) ([]cid.Cid, error) {
		nd, err := format.Decode(res)
		if err != nil {
			return nil, err
		}

		ids := make([]cid.Cid, len(nd.Links()))
		for i, l := range nd.Links() {
			ids[i] = l.Cid
		}

		return ids, nil
	})
}
