package blockstream

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type BlockStreamer interface {
	Stream(context.Context, <-chan []cid.Cid) <-chan blocks.Block
}
