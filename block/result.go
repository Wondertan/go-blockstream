package block

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type Result struct {
	blocks.Block

	Id  cid.Cid
	Err error
}

func (r Result) Cid() cid.Cid {
	if r.Block != nil {
		return r.Block.Cid()
	}

	return r.Id
}
