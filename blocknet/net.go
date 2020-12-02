package blocknet

import (
	"context"

	access "github.com/Wondertan/go-libp2p-access"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/Wondertan/go-blockstream/block"
)

const Protocol protocol.ID = "/blockstream/1.0.0"

type net struct {
	h  host.Host
	g  access.Granter
	m  RequestManager
	rh ResponderHandler
}

func NewNetwork(host host.Host, granter access.Granter, m RequestManager, rh ResponderHandler) Network {
	n := &net{h: host, g: granter, m: m, rh: rh}
	host.SetStreamHandler(Protocol, n.handle)
	return n
}

func (n *net) ID() peer.ID {
	return n.h.ID()
}

func (n *net) Requester(ctx context.Context, p peer.ID) (r *Requester, err error) {
	s, err := n.h.NewStream(ctx, p, Protocol)
	if err != nil {
		return
	}

	err = access.GiveHand(ctx, s)
	if err != nil {
		s.Reset()
		return
	}

	m := NewMessenger(s, p, func(f func() error) {
		err := f()
		if err != nil {
			log.Error("Messenger(%s) was terminated: %s", p.Pretty(), err)
			r.rq.ForEach(func(req block.Request) {
				req.Error(err)
			})
		}
	})

	return NewRequester(m), nil
}

func (n *net) handle(s network.Stream) {
	ctx, cancel := context.WithCancel(context.Background())
	p := s.Conn().RemotePeer()

	t, errs, err := access.TakeHand(n.g, s, p)
	if err != nil {
		s.Reset()
		return
	}

	ms := NewMessenger(s, p, func(f func() error) {
		err := f()
		if err != nil {
			log.Errorf("Messenger(%s) was terminated: %s", p.Pretty(), err)
			s.Reset()
			cancel()
			errs <- err
		}
	})

	n.rh(access.WithToken(ctx, t), NewResponder(ctx, ms, n.m))
}
