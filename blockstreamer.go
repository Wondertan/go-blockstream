package blockstream

import (
	"context"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// TODO Interface is not final
// TODO Implementation distributing requests between multiple streamers by some abstract characteristic.
type Streamer interface {

	// Stream initiates ordered stream of Blocks from implementation defined source.
	//
	// Err chan returns BlockError for requested blocks skipped on the response stream with reasonable error.
	// It has two purposes: obvious error handling and overcoming congestions.
	// If err chan returns any other error, the stream is terminated for some internal issue described by the error.
	Stream(context.Context, <-chan []cid.Cid) (<-chan blocks.Block, <-chan error)
}
