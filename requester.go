package blockstream

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"
)

// blockPutter is an interface responsible for saving blocks.
type blockPutter interface {
	PutMany([]blocks.Block) error
}

// requester is responsible for requesting block from a remote peer.
// It has to be paired with a responder on the other side of a conversation.
type requester struct {
	rwc io.ReadWriteCloser
	put blockPutter

	new, cncl chan *request
	rq        *requestQueue

	ctx    context.Context
	cancel context.CancelFunc
}

// newRequester creates new requester.
func newRequester(ctx context.Context, rwc io.ReadWriteCloser, reqs chan *request, put blockPutter, onErr onClose) *requester {
	ctx, cancel := context.WithCancel(ctx)
	rcv := &requester{
		rwc:    rwc,
		put:    put,
		new:    reqs,
		cncl:   make(chan *request),
		rq:     newRequestQueue(ctx.Done()),
		ctx:    ctx,
		cancel: cancel,
	}
	go onErr(rcv.writeLoop)
	go onErr(rcv.readLoop)
	return rcv
}

// writeLoop is a long running method which asynchronously handles requests, sends them to remote responder and queues up
// for future read by readLoop. It also handles request canceling, as well as request recovering in case stream is dead.
func (r *requester) writeLoop() error {
	defer func() {
		r.cancel()
		r.rwc.Close()
	}()

	for {
		select {
		case req := <-r.new:
			err := writeBlocksReq(r.rwc, req.Id(), req.Remains())
			if err != nil {
				select {
				case r.new <- req:
				case <-req.Done():
				case <-r.ctx.Done():
				}

				return fmt.Errorf("can't writeLoop request(%d): %w", req.id, err)
			}

			go r.onCancel(req)
			r.rq.Enqueue(req)
		case req := <-r.cncl:
			err := writeBlocksReq(r.rwc, req.Id(), nil)
			if err != nil {
				return fmt.Errorf("can't cancel request(%d): %w", req.id, err)
			}
		case <-r.ctx.Done():
			return nil
		}
	}
}

// readLoop is a long running method which receives requested blocks from the remote responder and fulfills queued request.
func (r *requester) readLoop() error {
	for {
		id, data, err := readBlocksResp(r.rwc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		req := r.rq.Back()
		if req == nil {
			_, err := r.rwc.Read([]byte{0})
			if !errors.Is(err, io.EOF) {
				return err
			}

			return nil
		}

		if req.Id() != id {
			log.Warnf("Received Blocks for wrong request(%d), skipping...", id)
			continue
		}

		ids := req.Remains()
		bs := make([]blocks.Block, len(data))
		for i, b := range data {
			bs[i], err = newBlockCheckCid(b, ids[i])
			if err != nil {
				if errors.Is(err, blocks.ErrWrongHash) {
					log.Errorf("%s: expected: %s, received: %s", err, ids[i], bs[i])
				}

				return err
			}
		}

		err = r.put.PutMany(bs)
		if err != nil {
			return err
		}

		if !req.Fill(bs) {
			r.rq.PopBack()
		}
	}
}

// onCancel handles request cancellation.
func (r *requester) onCancel(req *request) {
	select {
	case <-req.Done():
		if !req.Fulfilled() {
			select {
			case r.cncl <- req:
			case <-r.ctx.Done():
			}
		}
	case <-r.ctx.Done():
	}
}
