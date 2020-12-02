package blocknet

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream/block"
)

type request struct {
	r *Requester

	id   block.RequestID
	ids  []cid.Cid
	bs   chan *block.Option
	fld  int
	done chan struct{}
}

func (req *request) ID() block.RequestID {
	return req.id
}

func (req *request) Ids() []cid.Cid {
	return req.ids[req.fld:]
}

func (req *request) Peer() peer.ID {
	return req.r.ms.Peer()
}

func (req *request) Next() (*block.Option, bool) {
	if req.bs == nil {
		req.bs = make(chan *block.Option, len(req.ids))
		req.r.Send(req)
	}

	select {
	case o, ok := <-req.bs:
		return o, ok
	case <-req.Done():
		select {
		case o, ok := <-req.bs:
			return o, ok
		default:
			return nil, false
		}
	}
}

func (req *request) Fill(b *block.Option) bool {
	select {
	case req.bs <- b:
		req.fld++
		if req.fld == len(req.ids) {
			close(req.done)
			return false
		}
		return true
	case <-req.done:
		return false
	}
}

func (req *request) Error(err error) {
	for _, id := range req.Ids() {
		req.Fill(block.NewErrorOption(id, err))
	}
}

func (req *request) Cancel() {
	select {
	case <-req.done:
		return
	default:
	}

	req.r.Cancel(req.ID())
	close(req.done)
}

func (req *request) Have() {
	req.r.Have(req.ID())
}

func (req *request) Done() <-chan struct{} {
	return req.done
}
