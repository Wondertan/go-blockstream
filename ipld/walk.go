package ipld

import (
	"context"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"

	"github.com/Wondertan/go-blockstream"
)

type (
	// Handler is used to handle node in user defined way.
	Handler func(format.Node) error

	// Visitor checks whenever node should be fetched and handled.
	Visitor func(cid.Cid) (bool, error)
)

// Visit applies Visitor to check if specified node should be handled.
func Visit(visitor Visitor) walkOption {
	return func(w *walkOptions) {
		w.visitor = visitor
	}
}

// Handle applies custom Handler for specific node type.
func Handle(codec uint64, handle Handler) walkOption {
	return func(wo *walkOptions) {
		wo.handlers[codec] = handle
	}
}

// Walk traverses the DAG from given root passing all the nodes to the Handler.
func Walk(ctx context.Context, id cid.Cid, bs blockstream.BlockStreamer, handler Handler, opts ...walkOption) error {
	wo := options(opts)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remains := 1
	in := make(chan []cid.Cid, 1)
	in <- []cid.Cid{id}
	defer close(in)

	handle := func(nd format.Node) error {
		custom, ok := wo.handlers[nd.Cid().Type()]
		if ok {
			return custom(nd)
		}

		return handler(nd)
	}

	visit := func(cid cid.Cid) (bool, error) {
		if wo.visitor != nil {
			return wo.visitor(cid)
		}

		return true, nil
	}

	out := bs.Stream(ctx, in)
	for {
		select {
		case b := <-out:
			remains--

			nd, err := format.Decode(b)
			if err != nil {
				return err
			}

			err = handle(nd)
			if err != nil {
				return err
			}

			ls := nd.Links()
			if len(ls) == 0 {
				if remains == 0 {
					return nil
				}
				continue
			}

			ids := make([]cid.Cid, 0, len(ls))
			for _, l := range ls {
				v, err := visit(l.Cid)
				if err != nil {
					return err
				}
				if !v {
					continue
				}

				ids = append(ids, l.Cid)
			}
			remains += len(ids)

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

type walkOption func(*walkOptions)

type walkOptions struct {
	handlers map[uint64]Handler
	visitor  Visitor
}

func options(opts []walkOption) *walkOptions {
	wo := &walkOptions{
		handlers: make(map[uint64]Handler),
	}
	for _, opt := range opts {
		opt(wo)
	}
	return wo
}
