package blockstream

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// Explorer gets keys from block in a user defined way.
type Explorer func(blocks.Block) ([]cid.Cid, error)

// Explore gets first blocks from stream, passes it to handler that may explore new key in block and handles them over
// until no more left.
func Explore(ctx context.Context, id cid.Cid, bs Streamer, h Explorer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	remains := 1
	in := make(chan []cid.Cid, 8)
	in <- []cid.Cid{id}
	defer close(in)

	out, errCh := bs.Stream(ctx, in)
	for {
		select {
		case b, ok := <-out:
			if !ok {
				out = nil
				continue
			}

			remains--
			ids, err := h(b)
			if err != nil {
				return err
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
			case err := <-errCh:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
