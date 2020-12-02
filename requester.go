package blockstream

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/Wondertan/go-serde"
	"github.com/ipfs/go-block-format"

	"github.com/Wondertan/go-blockstream/block"
	"github.com/Wondertan/go-blockstream/blocknet"
)

// requester is responsible for requesting block from a remote peer.
// It has to be paired with a responder on the other side of a conversation.
type requester struct {
	rwc io.ReadWriteCloser

	new        chan *block.RequestGroup
	have, cncl chan block.RequestID
	rq         *block.RequestQueue

	ctx    context.Context
	cancel context.CancelFunc
}

// newRequester creates new requester.
func newRequester(ctx context.Context, rwc io.ReadWriteCloser, reqs chan *block.RequestGroup, onErr сlose) *requester {
	ctx, cancel := context.WithCancel(ctx)
	rcv := &requester{
		rwc:    rwc,
		new:    reqs,
		cncl:   make(chan *block.RequestGroup),
		rq:     block.NewRequestQueue(ctx.Done()),
		ctx:    ctx,
		cancel: cancel,
	}
	go onErr(rcv.writeLoop)
	go onErr(rcv.readLoop)
	return rcv
}

func (r *requester) Send(req *block.RequestGroup) error {
	select {
	case r.new <- req:
		return nil
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
}

func (r *requester) Have(id block.RequestID) {
	select {
	case r.have <- id:
	case <-r.ctx.Done():
	}
}

func (r *requester) Cancel(id block.RequestID) {
	select {
	case r.cncl <- id:
	case <-r.ctx.Done():
	}
}

// writeLoop is a long running method which asynchronously handles requests, sends them to remote responder and queues up
// for future read by readLoop. It also handles request canceling, as well as request recovering in case stream is dead.
func (r *requester) writeLoop() (err error) {
	defer r.cancel()
	for {
		select {
		case req := <-r.new:
			_, err = serde.Write(r.rwc, blocknet.newBlockRequestMsg(req))
			if err != nil {
				select {
				case r.new <- req:
				case <-req.Done():
				case <-r.ctx.Done():
				}

				return fmt.Errorf("can't writeLoop request(%d): %w", req.Id(), err)
			}

			r.rq.Enqueue(req)
		case id := <-r.have:
			_, err = serde.Write(r.rwc, blocknet.newHaveRequestMsg(id))
			if err != nil {
				return fmt.Errorf("can't write request(%s) have message: %w", id, err)
			}
		case id := <-r.cncl:
			_, err = serde.Write(r.rwc, blocknet.newCancelRequestMsg(id))
			if err != nil {
				return fmt.Errorf("can't write request(%s) cancel message: %w", id, err)
			}
		case <-r.ctx.Done():
			return r.rwc.Close()
		}
	}
}

// readLoop is a long running method which receives requested blocks from the remote responder and fulfills queued request.
func (r *requester) readLoop() error {
	for {
		id, data, reqErr, err := blocknet.readBlocksResp(r.rwc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		req := r.rq.BackPopDone()
		if req == nil {
			_, err := r.rwc.Read([]byte{0})
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if req.Id() != id {
			log.Warnf("Received Block response for wrong request(%d), skipping...", id)
			continue
		}

		if reqErr != nil {
			req.Error(reqErr)
			continue
		}

		ids := req.Remains()
		bs := make([]blocks.Block, len(data))
		for i, b := range data {
			bs[i], err = blocknet.newBlockCheckCid(b, ids[i])
			if err != nil {
				if errors.Is(err, blocks.ErrWrongHash) {
					log.Errorf("%s: expected: %s, received: %s", err, ids[i], bs[i])
				}

				return err
			}
		}

		if !req.Fill(bs) {
			r.rq.PopBack()
		}
	}
}
