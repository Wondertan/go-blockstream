package block

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type Option struct {
	Id cid.Cid
	Block blocks.Block
	Error error
}
