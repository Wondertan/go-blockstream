package blockstream

import (
	"context"
	"errors"
	"io"

	"github.com/Wondertan/go-blockstream/block"
)

// responder is responsible for responding to block requests from a remote peer.
// It must be paired with a requester on the side of a conversation.
type responder struct {
	rwc io.ReadWriteCloser

	reqs chan *block.Request
	rq   *block.RequestQueue

	ctx    context.Context
	cancel context.CancelFunc
}

// newResponder creates new responder.
func newResponder(ctx context.Context, rwc io.ReadWriteCloser, reqs chan *block.Request, onErr сlose) *responder {
	ctx, cancel := context.WithCancel(ctx)
	snr := &responder{
		rwc:    rwc,
		reqs:   reqs,
		rq:     block.NewRequestQueue(ctx.Done()),
		ctx:    ctx,
		cancel: cancel,
	}
	go onErr(snr.readLoop)
	go onErr(snr.writeLoop)
	return snr
}

// readLoop is a long running method which receives Block requests from the remote requester and queues them up
// for future write by writeLoop.
func (r *responder) readLoop() error {
	defer r.cancel()
	for {
		id, ids, err := readBlocksReq(r.rwc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if len(ids) == 0 {
			r.rq.Cancel(id)
			log.Debugf("[Requester] Request %d is cancelled", id)
			continue
		}

		// TODO Add limiting for both queues to exclude DOS vector, if it is reached - reset the stream
		log.Debugf("[Responder] Received request %d", id)
		req := block.NewRequest(r.ctx, id, ids)
		r.rq.Enqueue(req)
		select {
		case r.reqs <- req:
		case <-req.Done():
			return nil
		}
	}
}

// writeLoop is a long running method which takes queued requests and writes them as they are fulfilled.
func (r *responder) writeLoop() error {
	for {
		req := r.rq.Back()
		if req == nil {
			return r.rwc.Close()
		}

		for {
			bs, reqErr := req.Next()
			if errors.Is(reqErr, io.EOF) {
				break
			}

			err := writeBlocksResp(r.rwc, req.Id(), bs, reqErr)
			if err != nil {
				return err
			}
			log.Debugf("[Responder] Sent blocks for request %d", req.Id())
		}

		log.Debugf("[Responder] Request %d is fulfilled!", req.Id())
		r.rq.PopBack()
	}
}
