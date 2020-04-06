package blockstream

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
)

type Error struct {
	Peer  peer.ID
	Token Token
	Inner error
}

func NewError(peer peer.ID, token Token, err error) error {
	return &Error{Peer: peer, Token: token, Inner: err}
}

func (e *Error) Error() string {
	return fmt.Errorf("for %s with token(%s): %w", e.Peer.ShortString(), e.Token, e.Inner).Error()
}
