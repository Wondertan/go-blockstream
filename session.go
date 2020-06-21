package blockstream

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const (
	streamBufferSize  = 64
	streamBufferLimit = 1024
	requestBufferSize = 8
)

// blockTracker tracks blocks within the session
type blockTracker interface {
	blockPutter
	blockGetter
}

type Session struct {
	reqN, prvs uint32

	reqs chan *request
	trk  blockTracker

	ctx context.Context
}

func newSession(ctx context.Context, trk blockTracker) *Session {
	return &Session{
		reqs: make(chan *request, requestBufferSize),
		trk:  trk,
		ctx:  ctx,
	}
}

// Stream starts direct BBlock fetching from remote providers. It fetches the Blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also terminated with the provided context.
// Block order is guaranteed to be the same as requested through the `in` chan.
func (ses *Session) Stream(ctx context.Context, in <-chan []cid.Cid) <-chan blocks.Block {
	ctx, cancel := context.WithCancel(ctx)
	buf := NewBuffer(ctx, streamBufferSize, streamBufferLimit)
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
					log.Error(err)
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

	buf := NewBuffer(ctx, len(ids), len(ids))
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
	in, err := ses.tracked(ctx, in, out)
	if len(in) == 0 || err != nil {
		return err
	}

	sets := ses.distribute(in)
	reqs := make([]*request, len(sets))
	for i, set := range sets {
		reqs[i] = newRequestWithChan(ctx, ses.requestId(), set, out)
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

// tracked sends known Blocks to the chan and returns ids remained to be fetched.
func (ses *Session) tracked(ctx context.Context, in []cid.Cid, out chan<- []blocks.Block) ([]cid.Cid, error) {
	var hv, dhv []cid.Cid // need to make a copy
	for _, id := range in {
		if !id.Defined() {
			continue
		}

		ok, err := ses.trk.Has(id)
		if err != nil {
			return nil, err
		}

		if ok {
			hv = append(hv, id)
		} else {
			dhv = append(dhv, id)
		}
	}

	if len(hv) == 0 {
		return dhv, nil
	}

	go func() {
		bs := make([]blocks.Block, 0, len(hv))
		for i, id := range hv {
			b, err := ses.trk.Get(id)
			if err != nil {
				log.Errorf("Can't get tracked block: %s", err)
				continue
			}

			bs[i] = b
		}

		select {
		case out <- bs:
		case <-ctx.Done():
			return
		case <-ses.ctx.Done():
			return
		}
	}()

	return dhv, nil
}

func (ses *Session) addProvider(rwc io.ReadWriteCloser, closing onClose) {
	newRequester(ses.ctx, rwc, ses.reqs, ses.trk, closing)
	atomic.AddUint32(&ses.prvs, 1)
}

func (ses *Session) removeProvider() {
	atomic.AddUint32(&ses.prvs, ^uint32(0))
}

func (ses *Session) requestId() uint32 {
	return atomic.AddUint32(&ses.reqN, 1)
}
