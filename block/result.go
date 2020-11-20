package block

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type Result struct {
	cid.Cid
	Block blocks.Block
	Error error
}

func (res Result) Get() (blocks.Block, error) {
	return res.Block, res.Error
}
