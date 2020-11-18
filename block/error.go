package block

import (
	"fmt"
	"github.com/ipfs/go-cid"
)

type Error struct {
	Cid cid.Cid
	Err error
}

func (br *Error) Error() string {
	return fmt.Sprintf("blockstream: failed to process block %s: %s", br.Cid, br.Err)
}

func (br *Error) Unwrap() error {
	return br.Err
}
