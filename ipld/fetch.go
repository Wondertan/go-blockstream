package ipld

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

func FetchAll(ctx context.Context, id cid.Cid, dag format.NodeAdder, ses blockstream.BlockStreamer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remain := 1
	in := make(chan []cid.Cid, 1)
	in <- []cid.Cid{id}
	defer close(in)

	out := ses.Stream(ctx, in)
	for {
		select {
		case b := <-out:
			remain--

			nd, err := format.Decode(b)
			if err != nil {
				return err
			}

			err = dag.Add(ctx, nd)
			if err != nil {
				return err
			}

			ls := nd.Links()
			ll := len(ls)
			if ll == 0 {
				if remain == 0 {
					return nil
				}
				continue
			}
			remain += ll

			ids := make([]cid.Cid, ll)
			for i, l := range ls {
				ids[i] = l.Cid
			}

			select {
			case in <- ids:
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
