package ipld

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

type Visitor func(format.Node) error

type WalkOption func(*walkOptions)

func Visit(codec uint64, visit Visitor) WalkOption {
	return func(wo *walkOptions) {
		wo.visitors[codec] = visit
	}
}

func VisitAll(visit Visitor) WalkOption {
	return func(wo *walkOptions) {
		wo.all = visit
	}
}

// Walk traverses the DAG from given root visiting all the nodes with the Visitor.
// Calling visitor is thread safe.
func Walk(ctx context.Context, id cid.Cid, bs blockstream.BlockStreamer, opts ...WalkOption) error {
	wo := options(opts)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remains := 1
	in := make(chan []cid.Cid, 1)
	in <- []cid.Cid{id}
	defer close(in)

	out := bs.Stream(ctx, in)
	for {
		select {
		case b := <-out:
			remains--

			nd, err := format.Decode(b)
			if err != nil {
				return err
			}

			visit, ok := wo.visitors[nd.Cid().Type()]
			if ok {
				err = visit(nd)
				if err != nil {
					return err
				}
			}

			if wo.all != nil {
				err = wo.all(nd)
				if err != nil {
					return err
				}
			}

			ls := nd.Links()
			ll := len(ls)
			if ll == 0 {
				if remains == 0 {
					return nil
				}
				continue
			}
			remains += ll

			select {
			case in <- linksToCids(ls):
			case <-ctx.Done():
				return ctx.Err()
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func linksToCids(ls []*format.Link) []cid.Cid {
	ids := make([]cid.Cid, len(ls))
	for i, l := range ls {
		ids[i] = l.Cid
	}

	return ids
}

type walkOptions struct {
	all      Visitor
	visitors map[uint64]Visitor
}

func options(opts []WalkOption) *walkOptions {
	wo := &walkOptions{
		visitors: make(map[uint64]Visitor),
	}
	for _, opt := range opts {
		opt(wo)
	}
	return wo
}
