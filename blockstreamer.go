package blockstream

import (
	"context"
	blockstore "github.com/ipfs/go-ipfs-blockstore"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// TODO Implementation distributing requests between multiple streamers by some abstract characteristic.
type BlockStreamer interface {

	// Stream initiates ordered stream of Blocks from implementation defined source.
	//
	// Err chan returns BlockError for requested blocks skipped on the response stream with reasonable error.
	// It has two purposes: obvious error handling and overcoming congestions.
	// If err chan returns any other error, the stream is terminated for some internal issue described by the error.
	Stream(context.Context, <-chan []cid.Cid) (<-chan blocks.Block, <-chan error)
}

type BlockstoreStreamer struct {
	bs blockstore.Blockstore
}

func NewBlockstoreStreamer(bs blockstore.Blockstore) BlockStreamer {
	return &BlockstoreStreamer{
		bs: bs,
	}
}

func (b *BlockstoreStreamer) Stream(ctx context.Context, in <-chan []cid.Cid) (<-chan blocks.Block, <-chan error) {
	outB, outErr := make(chan blocks.Block, len(in)), make(chan error, 1)

	// TODO Spawn multiple routines to accelerate Stream for case when more Ps are available.
	go func() {
		select {
		case ids, ok := <-in:
			if !ok {
				return
			}

			for _, id := range ids {
				b, err := b.bs.Get(id)
				if err != nil {
					select {
					case outErr <- &BlockError{Cid: id, Err: err}:
					case <-ctx.Done():
						return
					}
				}

				select {
				case outB <- b:
				case <-ctx.Done():
					return
				}
			}

		case <-ctx.Done():
			return
		}
	}()

	return outB, outErr
}
