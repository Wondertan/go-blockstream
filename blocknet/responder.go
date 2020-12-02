package blocknet

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream/block"
)

type RequestManager interface {
	Request(context.Context, block.RequestID, peer.ID, ...cid.Cid) block.Request
}

type Responder struct {
	ctx  context.Context
	ms   *Messenger
	rm   RequestManager
	rq   *block.RequestQueue
	reqs map[block.RequestID]block.Request
}

func NewResponder(ctx context.Context, ms *Messenger, rm RequestManager) *Responder {
	r := &Responder{
		ctx:  ctx,
		ms:   ms,
		rm:   rm,
		rq:   block.NewRequestQueue(ms.Done()),
		reqs: make(map[block.RequestID]block.Request),
	}

	ms.OnMessage(REQUEST, r.onRequest)
	ms.OnMessage(HAVE, r.onHave)
	ms.OnMessage(CANCEL, r.onCancel)
	go r.respond()
	return r
}

func (r *Responder) Shut(ctx context.Context) error {
	return nil
}

func (r *Responder) onRequest(msg Message) {
	id, ids, err := BlocksRequest(msg)
	if err != nil {
		log.Error("Wrong message: %s", err)
		return
	}

	r.rq.Enqueue(r.request(id, ids...))
}

func (r *Responder) onHave(msg Message) {
	id, err := HaveRequest(msg)
	if err != nil {
		log.Error("Wrong message: %s", err)
		return
	}

	r.request(id).Have()
}

func (r *Responder) onCancel(msg Message) {
	id, err := CancelRequest(msg)
	if err != nil {
		log.Error("Wrong message: %s", err)
		return
	}

	r.request(id).Cancel()
}

func (r *Responder) request(id block.RequestID, ids ...cid.Cid) block.Request {
	req, ok := r.reqs[id]
	if !ok {
		req = r.rm.Request(r.ctx, id, r.ms.Peer(), ids...)
		r.reqs[id] = req
	}

	return req
}

func (r *Responder) respond() {
	for {
		req := r.rq.Back()
		if req == nil {
			err := r.ms.Close()
			if err != nil {
				log.Errorf("Can't close messenger: %s", err)
			}

			return
		}

		msg, ok := r.response(req)
		if ok {
			err := r.ms.Send(msg)
			if err != nil {
				log.Errorf("Can't send response for request(%s): %s", req.ID(), err)
				return
			}
		}

		r.rq.PopBack()
	}
}

func (r *Responder) response(req block.Request) (Message, bool) {
	var (
		size int
		bs   []blocks.Block
	)
	for {
		o, ok := req.Next()
		if !ok {
			if len(bs) > 0 {
				return BlocksResponseMsg(req.ID(), bs), true
			}

			return nil, false
		}

		b, err := o.Get()
		if err != nil {
			return ErrorResponseMsg(req.ID(), err), true
		}
		bs = append(bs, b)

		size += len(b.RawData())
		if size >= maxMessageSize {
			return BlocksResponseMsg(req.ID(), bs), true
		}
	}
}
