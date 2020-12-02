package blockstream

import (
	"context"
	"errors"
	"io"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/blocknet"
)

// responder is responsible for responding to block requests from a remote peer.
// It must be paired with a requester on the side of a conversation.
type responder struct {
	rwc io.ReadWriteCloser

	reqs chan *block.RequestGroup
	rq   *block.RequestQueue

	ctx    context.Context
	cancel context.CancelFunc
}

// newResponder creates new responder.
func newResponder(ctx context.Context, rwc io.ReadWriteCloser, reqs chan *block.RequestGroup, onErr сlose) *responder {
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
		id, ids, err := blocknet.readBlocksReq(r.rwc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if len(ids) == 0 {
			r.rq.Cancel(id)
			continue
		}

		// TODO Add limiting for both queues to exclude DOS vector, if it is reached - reset the stream
		req, err := block.NewRequestGroup(r.ctx, id, ids)
		if err != nil {
			return err
		}

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

			err := blocknet.writeBlocksResp(r.rwc, req.Id(), bs, reqErr)
			if err != nil {
				return err
			}
		}

		r.rq.PopBack()
	}
}
