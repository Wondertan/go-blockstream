package blockstream

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// blocksBufLen defines length of a request block chan buffer.
const blocksBufLen = 4

// request is a tuple of request request params.
type request struct {
	id, fld uint32

	bs  chan []blocks.Block
	ids []cid.Cid

	ctx    context.Context
	cancel context.CancelFunc
}

// newRequest creates new request.
func newRequest(ctx context.Context, id uint32, ids []cid.Cid) *request {
	return newRequestWithChan(ctx, id, ids, make(chan []blocks.Block, blocksBufLen))
}

// newRequest creates new request with given blocks channel.
func newRequestWithChan(ctx context.Context, id uint32, ids []cid.Cid, bs chan []blocks.Block) *request {
	ctx, cancel := context.WithCancel(ctx)
	return &request{id: id, bs: bs, ids: ids, ctx: ctx, cancel: cancel}
}

// Id returns request's id.
func (req *request) Id() uint32 {
	return req.id
}

// Fulfilled checks whenever request is fully filled and finished.
func (req *request) Fulfilled() bool {
	return atomic.LoadUint32(&req.fld) == uint32(len(req.ids))
}

// Done returns done channel of underlying context.
func (req *request) Done() <-chan struct{} {
	return req.ctx.Done()
}

// Cancel finishes the request.
func (req *request) Cancel() {
	req.cancel()
}

// Remains returns remaining ids for the request to become fulfilled.
func (req *request) Remains() []cid.Cid {
	return req.ids[atomic.LoadUint32(&req.fld):]
}

// Next waits for new incoming blocks.
// Also returns false when request is fulfilled.
func (req *request) Next() ([]blocks.Block, bool) {
	select {
	case bs := <-req.bs:
		if req.Fulfilled() && len(req.bs) == 0 { // TODO I don't like this check
			req.Cancel()
		}

		return bs, true
	case <-req.ctx.Done():
		return nil, false
	}
}

// Fill fills up the request with asked blocks.
func (req *request) Fill(bs []blocks.Block) bool {
	if bs == nil {
		return false
	}

	select {
	case req.bs <- bs:
		if atomic.AddUint32(&req.fld, uint32(len(bs))) == uint32(len(req.ids)) {
			return false
		}
		return true
	case <-req.ctx.Done():
		return false
	}
}

type requestQueue struct {
	l    *list.List
	m    sync.RWMutex
	sig  chan struct{}
	done <-chan struct{}
}

func newRequestQueue(done <-chan struct{}) *requestQueue {
	return &requestQueue{
		l:    list.New(),
		sig:  make(chan struct{}, 1),
		done: done,
	}
}

func (rq *requestQueue) Len() int {
	rq.m.RLock()
	defer rq.m.RUnlock()
	return rq.l.Len()
}

func (rq *requestQueue) Enqueue(req *request) {
	rq.m.Lock()
	defer rq.m.Unlock()

	rq.l.PushFront(req)
	rq.signal()
}

func (rq *requestQueue) Back() *request {
	for {
		rq.m.RLock()
		if rq.l.Back() != nil {
			rq.signal()
		}
		rq.m.RUnlock()

		select {
		case <-rq.sig:
			rq.m.RLock()
			e := rq.l.Back()
			rq.m.RUnlock()

			req := e.Value.(*request)
			select {
			case <-req.Done():
				rq.m.Lock()
				rq.l.Remove(e)
				rq.m.Unlock()
				continue
			default:
				return req
			}
		case <-rq.done:
			return nil
		}
	}
}

func (rq *requestQueue) PopBack() {
	rq.m.Lock()
	rq.l.Remove(rq.l.Back())
	rq.m.Unlock()
}

func (rq *requestQueue) Cancel(id uint32) {
	req := rq.get(id)
	if req != nil {
		req.Cancel()
	}
}

func (rq *requestQueue) get(id uint32) *request {
	rq.m.RLock()
	defer rq.m.RUnlock()

	for e := rq.l.Front(); e != nil; e = e.Next() {
		if e.Value.(*request).id == id {
			return e.Value.(*request)
		}
	}

	return nil
}

func (rq *requestQueue) signal() {
	select {
	case rq.sig <- struct{}{}:
	default:
	}
}
