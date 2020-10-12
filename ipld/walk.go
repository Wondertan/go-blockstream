package ipld

import (
	"context"
	"errors"

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

// NoDedup forces Walk to request and handle nodes more than once if they are already passed through.
func NoDedup() walkOption {
	return func(wo *walkOptions) {
		wo.dedup = nil
	}
}

func StreamInBuf(len int) walkOption {
	return func(w *walkOptions) {
		w.cidChanLen = len
	}
}

// Walk traverses the DAG from given root passing all the nodes to the Handler.
func Walk(ctx context.Context, id cid.Cid, bs blockstream.BlockStreamer, handler Handler, opts ...walkOption) error {
	wo := options(handler, opts)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remains := 1
	in := make(chan []cid.Cid, wo.cidChanLen)
	in <- []cid.Cid{id}
	defer close(in)

	out, cherr := bs.Stream(ctx, in)
	for {
		select {
		case b, ok := <-out:
			if !ok {
				return errors.New("stream channel closed")
			}
			remains--

			nd, err := format.Decode(b)
			if err != nil {
				return err
			}

			shouldHandle := true
			if nd.Cid().Type() != cid.Raw {
				shouldHandle, err = wo.visit(nd.Cid())
				if err != nil {
					return err
				}
			}

			if shouldHandle {
				err = wo.handle(nd)
				if err != nil {
					return err
				}
			}

			ids := make([]cid.Cid, 0, len(nd.Links()))
			for _, l := range nd.Links() {
				if l.Cid.Type() != cid.Raw {
					ids = append(ids, l.Cid)
					continue
				}

				v, err := wo.visit(l.Cid)
				if err != nil {
					return err
				}
				if v {
					ids = append(ids, l.Cid)
				}
			}

			if len(ids) == 0 {
				if remains == 0 {
					return nil
				}
				continue
			}
			remains += len(ids)

			select {
			case in <- ids:
			case <-ctx.Done():
				return ctx.Err()
			case err := <-cherr:
				cancel()
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case err := <-cherr:
			cancel()
			return err
		}
	}
}

type walkOption func(*walkOptions)

type walkOptions struct {
	cidChanLen int
	dedup      *cid.Set
	visitor    Visitor
	handlers   map[uint64]Handler
	main       Handler
}

func options(main Handler, opts []walkOption) *walkOptions {
	wo := &walkOptions{
		cidChanLen: 16,
		dedup:      cid.NewSet(),
		handlers:   make(map[uint64]Handler),
		main:       main,
	}
	for _, opt := range opts {
		opt(wo)
	}
	return wo
}

func (wo *walkOptions) handle(nd format.Node) error {
	custom, ok := wo.handlers[nd.Cid().Type()]
	if ok {
		return custom(nd)
	}

	return wo.main(nd)
}

func (wo *walkOptions) visit(id cid.Cid) (bool, error) {
	if wo.dedup != nil && wo.dedup.Has(id) {
		return false, nil
	}

	wo.dedup.Add(id)
	if wo.visitor != nil {
		return wo.visitor(id)
	}

	return true, nil
}
