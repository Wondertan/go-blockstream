package blockstream

import (
	"context"
	"github.com/Wondertan/go-blockstream/block"
	"github.com/ipfs/go-cid"
)

// TODO Interface is not final
// TODO Implementation distributing requests between multiple streamers by some abstract characteristic.
type Streamer interface {

	// Stream initiates ordered stream of Blocks from implementation defined source.
	Stream(context.Context, <-chan []cid.Cid) (<-chan block.Result, <-chan error)
}
