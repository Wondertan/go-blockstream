package blockstream

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/Wondertan/go-blockstream/block"
)

var (
	StreamBufferSize = 128
)

const requestBufferSize = 8

type Session struct {
	reqN, prvs uint32

	reqs  chan *block.Request
	cache block.Cache

	ctx context.Context
}

func newSession(ctx context.Context, cache block.Cache) *Session {
	return &Session{
		reqs:  make(chan *block.Request, requestBufferSize),
		cache: cache,
		ctx:   ctx,
	}
}

// Stream starts direct BBlock fetching from remote providers. It fetches the Blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also terminated with the provided context.
// Block order is guaranteed to be the same as requested through the `in` chan.
func (ses *Session) Stream(ctx context.Context, in <-chan []cid.Cid) <-chan blocks.Block {
	ctx, cancel := context.WithCancel(ctx)
	buf := block.NewStream(ctx, ses.cache, StreamBufferSize)
	go func() {
		defer buf.Close()
		for {
			select {
			case ids, ok := <-in:
				if !ok {
					return
				}

				err := buf.Enqueue(ids...)
				if err != nil {
					return
				}

				err = ses.request(ctx, ids, buf.Input())
				if err != nil {
					return
				}
			case <-ses.ctx.Done():
				cancel()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return buf.Output()
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

	buf := block.NewStream(ctx, ses.cache, len(ids))
	err := buf.Enqueue(ids...)
	if err != nil {
		return nil, err
	}

	err = ses.request(ctx, ids, buf.Input())
	if err != nil {
		return nil, err
	}

	return buf.Output(), buf.Close()
}

// request requests providers in the session for Blocks and writes them out to the chan.
func (ses *Session) request(ctx context.Context, in []cid.Cid, out chan []blocks.Block) error {
	in = ses.cached(in)
	if len(in) == 0 {
		return nil
	}

	sets := ses.distribute(in)
	reqs := make([]*block.Request, len(sets))
	for i, set := range sets {
		reqs[i] = block.NewRequestWithChan(ctx, ses.requestId(), set, out)
	}

	for _, req := range reqs {
		select {
		case ses.reqs <- req:
		case <-ses.ctx.Done():
			return ses.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// distribute splits ids between providers to download from multiple sources.
func (ses *Session) distribute(ids []cid.Cid) [][]cid.Cid {
	l := int(atomic.LoadUint32(&ses.prvs))
	sets := make([][]cid.Cid, l)
	for i, k := range ids {
		sets[i%l] = append(sets[i%l], k)
	}

	return sets
}

// cached checks known Blocks and returns ids remained to be fetched.
func (ses *Session) cached(in []cid.Cid) []cid.Cid {
	var out []cid.Cid // need to make a copy
	for _, id := range in {
		if !id.Defined() {
			continue
		}

		if !ses.cache.Has(id) {
			out = append(out, id)
		}
	}

	return out
}

func (ses *Session) addProvider(rwc io.ReadWriteCloser, closing сlose) {
	newRequester(ses.ctx, rwc, ses.reqs, closing)
	atomic.AddUint32(&ses.prvs, 1)
}

func (ses *Session) removeProvider() {
	atomic.AddUint32(&ses.prvs, ^uint32(0))
}

func (ses *Session) requestId() uint32 {
	return atomic.AddUint32(&ses.reqN, 1)
}
