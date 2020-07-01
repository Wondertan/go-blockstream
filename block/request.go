package block

import (
	"context"
	"io"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// blocksBufLen defines length of a Request block chan buffer.
const blocksBufLen = 8

// Request is a tuple of Request Request params.
type Request struct {
	id, fld uint32

	bs  chan []blocks.Block
	ids []cid.Cid

	err    error
	done   <-chan struct{}
	cancel context.CancelFunc
}

// NewRequest creates new Request.
func NewRequest(ctx context.Context, id uint32, ids []cid.Cid) *Request {
	return NewRequestWithChan(ctx, id, ids, make(chan []blocks.Block, blocksBufLen))
}

// NewRequestWithChan creates new Request with given blocks channel.
func NewRequestWithChan(ctx context.Context, id uint32, ids []cid.Cid, bs chan []blocks.Block) *Request {
	ctx, cancel := context.WithCancel(ctx)
	return &Request{id: id, bs: bs, ids: ids, done: ctx.Done(), cancel: cancel}
}

// Id returns Request's id.
func (req *Request) Id() uint32 {
	return req.id
}

// Fulfilled checks whenever Request is fully filled and finished.
func (req *Request) Fulfilled() bool {
	return atomic.LoadUint32(&req.fld) == uint32(len(req.ids))
}

// Done returns done channel of underlying context.
func (req *Request) Done() <-chan struct{} {
	return req.done
}

// Cancel finishes the Request.
func (req *Request) Cancel() {
	req.cancel()
}

// Remains returns remaining ids for the Request to become fulfilled.
func (req *Request) Remains() []cid.Cid {
	return req.ids[atomic.LoadUint32(&req.fld):]
}

// Next waits for new incoming blocks.
// Also returns false when Request is fulfilled.
func (req *Request) Next() ([]blocks.Block, error) {
	select {
	case bs := <-req.bs:
		return bs, nil
	case <-req.Done():
		select {
		case bs := <-req.bs:
			return bs, nil
		default:
			if req.err != nil {
				return nil, req.err
			}

			return nil, io.EOF
		}
	}
}

// Fill fills up the Request with asked blocks.
func (req *Request) Fill(bs []blocks.Block) bool {
	if bs == nil {
		return false
	}

	select {
	case req.bs <- bs:
		if atomic.AddUint32(&req.fld, uint32(len(bs))) == uint32(len(req.ids)) {
			req.Cancel()
			return false
		}

		return true
	case <-req.Done():
		return false
	}
}

// Error cancels the request with an error.
func (req *Request) Error(err error) {
	if err == nil {
		return
	}

	select {
	case <-req.Done():
		return
	default:
		req.err = err
		req.Cancel()
	}
}
