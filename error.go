package blockstream

import (
	"fmt"
	"github.com/ipfs/go-cid"
)

type BlockError struct {
	Cid cid.Cid
	Err error
}

func (br *BlockError) Error() string {
	return fmt.Sprintf("blockstream: failed to process block %s: %s", br.Cid, br.Err)
}

func (br *BlockError) Unwrap() error {
	return br.Err
}
