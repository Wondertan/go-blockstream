package block

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

type Option struct {
	id  cid.Cid
	b   blocks.Block
	err error
}

func NewSuccessOption(b blocks.Block) *Option {
	return &Option{
		id: b.Cid(),
		b:  b,
	}
}

func NewErrorOption(id cid.Cid, err error) *Option {
	return &Option{
		id:  id,
		err: err,
	}
}

func (o *Option) Cid() cid.Cid {
	return o.id
}

func (o *Option) Get() (blocks.Block, error) {
	if o.err != nil {
		return nil, o.err
	}

	return o.b, nil
}

func (o *Option) Empty() bool {
	return o.b == nil
}
