package blockstream

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/Wondertan/go-blockstream/block"
)

const requestBufferSize = 8

type Session struct {
	reqN, prvs uint32

	reqs   chan *block.Request
	ctx    context.Context
	cancel context.CancelFunc

	err error
}

func newSession(ctx context.Context) *Session {
	ctx, cancel := context.WithCancel(ctx)
	return &Session{
		reqs:   make(chan *block.Request, requestBufferSize),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Stream starts direct Block fetching from remote providers. It fetches the Blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also terminated with the provided context.
// Block order is guaranteed to be the same as requested through the `in` chan.
func (ses *Session) Stream(ctx context.Context, in <-chan []cid.Cid) (<-chan blocks.Block, <-chan error) {
	ctx, cancel := context.WithCancel(ctx)
	s := block.NewStream(ctx)

	err := make(chan error, 1)
	go func() {
		defer close(err)

		for {
			select {
			case ids, ok := <-in:
				if !ok {
					return
				}

				reqs := ses.request(ctx, ids)
				if len(reqs) == 0 {
					continue
				}

				s.Enqueue(reqs...)
			case <-ses.ctx.Done():
				cancel()
				if ses.err != nil {
					err <- ses.err
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return s.Output(), err
}

// Blocks fetches Blocks by their CIDs evenly from the remote providers in the session.
// Block order is guaranteed to be the same as requested.
func (ses *Session) Blocks(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, error) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-ses.ctx.Done(): // not to leak Buffer in case session context is closed
			cancel()
		case <-ctx.Done():
		}
	}()

	reqs := ses.request(ctx, ids)
	if len(reqs) == 0 {
		ch := make(chan blocks.Block)
		close(ch)
		return ch, nil
	}

	s := block.NewStream(ctx)
	s.Enqueue(reqs...)
	return s.Output(), nil
}

// request requests providers in the session for Blocks and writes them out to the chan.
func (ses *Session) request(ctx context.Context, ids []cid.Cid) (reqs []*block.Request) {
	sets := ses.distribute(ids)
	reqs = make([]*block.Request, 0, len(sets))
	for _, set := range sets {
		if len(set) == 0 {
			continue
		}

		req := block.NewRequest(ctx, ses.requestId(), set)
		select {
		case ses.reqs <- req:
			reqs = append(reqs, req)
		case <-ses.ctx.Done():
			return
		case <-ctx.Done():
			return
		}
	}

	return
}

// distribute splits ids between providers to download from multiple sources.
func (ses *Session) distribute(ids []cid.Cid) [][]cid.Cid {
	prs, l := int(atomic.LoadUint32(&ses.prvs)), len(ids)
	sets := make([][]cid.Cid, prs)
	for i := 0; i < prs; i++ {
		sets[i] = ids[i*l/prs : (i+1)*l/prs]
	}

	return sets
}

func (ses *Session) addProvider(rwc io.ReadWriteCloser, closing сlose) {
	newRequester(ses.ctx, rwc, ses.reqs, closing)
	atomic.AddUint32(&ses.prvs, 1)
}

func (ses *Session) removeProvider() {
	atomic.AddUint32(&ses.prvs, ^uint32(0))
}

func (ses *Session) getProviders() uint32 {
	return atomic.LoadUint32(&ses.prvs)
}

func (ses *Session) requestId() uint32 {
	return atomic.AddUint32(&ses.reqN, 1)
}
