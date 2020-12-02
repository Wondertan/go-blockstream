package blocknet

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream/test"
)

var CloseLogger CloseHandler = func(f func() error) {
	err := f()
	if err != nil {
		log.Error(err)
	}
}

func NewTestMessengerPair() (*Messenger, *Messenger) {
	s1, s2 := test.StreamPair()
	return NewMessenger(s1, peer1, CloseLogger), NewMessenger(s2, peer2, CloseLogger)
}

var (
	peer1 peer.ID = "peer1"
	peer2 peer.ID = "peer2"
)

func newTestRequester() (*Requester, *Messenger) {
	m1, m2 := NewTestMessengerPair()
	return NewRequester(m1), m2
}

func newTestResponder(rm RequestManager) (*Responder, *Messenger) {
	m1, m2 := NewTestMessengerPair()
	return NewResponder(m1, rm), m2
}

func newFakeMessage(t *testing.T) Message {
	bs, ids := test.RandBlocks(t, rand.Reader, 10, 256)
	return BlocksResponseMsg(ids[0], bs)
}
