package block

import (
	"context"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type RequestID = cid.Cid

type Request interface {
	ID() RequestID
	Ids() []cid.Cid
	Peer() peer.ID
	Next() (*Option, bool)
	Fill(*Option) bool
	Error(error)
	Cancel()
	Have()
	Done() <-chan struct{}
}

type RequestGroup struct {
	id RequestID

	remote   map[peer.ID]Request
	remoteCh chan []Request

	local   map[peer.ID]*request
	localCh chan *request

	have, cancl chan peer.ID
	ids         chan []cid.Cid

	err chan error
	ctx context.Context

	blocks []*Option
	in     chan blocks.Block

	srvd  int
	fld   uint32
	fldCh chan struct{}
}

// NewRequest creates new Request.
func NewRequestGroup(ctx context.Context, id RequestID) *RequestGroup {
	r := &RequestGroup{
		id:       id,
		remote:   make(map[peer.ID]Request),
		remoteCh: make(chan []Request, 1),
		local:    make(map[peer.ID]*request),
		localCh:  make(chan *request, 1),
		have:     make(chan peer.ID, 1),
		cancl:    make(chan peer.ID, 1),
		err:      make(chan error, 1),
		ctx:      ctx,
		fldCh:    make(chan struct{}),
	}

	go r.handle()
	return r
}

func (rg *RequestGroup) Init() bool {
	return rg.blocks != nil
}

// Id returns Request's id.
func (rg *RequestGroup) Id() RequestID {
	return rg.id
}

func (rg *RequestGroup) Fill(b blocks.Block) bool {
	if b == nil {
		return false
	}

	select {
	case rg.in <- b:
		if int(atomic.AddUint32(&rg.fld, 1)) == len(rg.blocks) {
			close(rg.fldCh)
			return false
		}

		return true
	case <-rg.fldCh:
	case <-rg.ctx.Done():
	}

	return false
}

type request struct {
	rg   *RequestGroup
	peer peer.ID
	out  chan *Option
	done chan struct{}
}

func (req *request) ID() RequestID {
	return req.rg.id
}

func (req *request) Peer() peer.ID {
	return req.peer
}

func (req *request) Ids() []cid.Cid {
	return nil
}

func (req *request) Next() (*Option, bool) {
	if req.out == nil {
		req.out = make(chan *Option, len(req.rg.blocks))
		select {
		case req.rg.localCh <- req:
		case <-req.rg.ctx.Done():
			return nil, false
		}
	}

	select {
	case o, ok := <-req.out:
		if ok {
			return o, true
		}
	case <-req.done:
	}

	return nil, false
}

func (req *request) Fill(o *Option) bool {
	return false
}

func (req *request) Error(err error) {
	select {
	case req.rg.err <- err:
	case <-req.rg.fldCh:
	case <-req.rg.ctx.Done():
	}
}

func (req *request) Have() {
	select {
	case req.rg.have <- req.peer:
	case <-req.rg.ctx.Done():
	}
}

func (req *request) Cancel() {
	select {
	case req.rg.cancl <- req.peer:
	case <-req.rg.ctx.Done():
	}

	close(req.done)
}

func (req *request) Done() <-chan struct{} {
	return req.done
}

func (rg *RequestGroup) SetIds(ids []cid.Cid) {
	select {
	case rg.ids <- ids:
	case <-rg.ctx.Done():
	}
}

func (rg *RequestGroup) Add(reqs ...Request) {
	select {
	case rg.remoteCh <- reqs:
	case <-rg.ctx.Done():
	}
}

func (rg *RequestGroup) RequestFor(ctx context.Context, p peer.ID) Request {
	req := &request{
		rg:   rg,
		peer: p,
		done: make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.Done():
			req.Cancel()
		}
	}()

	return req
}

func (rg *RequestGroup) handle() {
	for {
		select {
		case ids := <-rg.ids:
			rg.blocks = make([]*Option, len(ids))
			for i, id := range ids {
				rg.blocks[i] = &Option{
					id: id,
				}
			}
		case req := <-rg.localCh:
			for _, b := range rg.blocks[:rg.srvd] {
				req.out <- b
			}

			if rg.srvd == len(rg.blocks) {
				close(req.out)
			}

			rg.local[req.Peer()] = req
		case reqs := <-rg.remoteCh:
			for _, req := range reqs {
				// TODO Send HAVE if filled
				rg.remote[req.Peer()] = req
			}
		case b := <-rg.in:
			for i, bo := range rg.blocks[rg.srvd:] {
				if bo.b == nil && bo.id.Equals(b.Cid()) {
					bo.b = b
					if i == 0 {
						for _, b := range rg.blocks[rg.srvd:] {
							if b.b == nil {
								break
							}

							for _, pr := range rg.local {
								pr.out <- b
							}

							rg.srvd++
						}

						if rg.srvd == len(rg.blocks) {
							for _, pr := range rg.local {
								close(pr.out)
							}
						}
					}
					break
				}
			}
		case err := <-rg.err:
			for _, b := range rg.blocks[rg.srvd:] {
				b.err = err

				for _, pr := range rg.local {
					pr.out <- b
				}

				rg.srvd++
			}

			for _, pr := range rg.local {
				close(pr.out)
			}

		case _ = <-rg.have:

		case _ = <-rg.cancl:

		case <-rg.fldCh:
			for _, r := range rg.remote {
				r.Have()
			}
		case <-rg.ctx.Done():
			for _, r := range rg.remote {
				r.Cancel()
			}
		}
	}
}
