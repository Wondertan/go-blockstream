package blockstream

import (
	"context"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"io"
	"sync/atomic"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/Wondertan/go-blockstream/block"
)

const (
	maxAvailableWorkers = 128
	requestBufferSize = 8
)

// TODO Refactor this, my ayes hurt watching this
type Session struct {
	ctx    context.Context
	cancel context.CancelFunc

	reqN, prvs uint32
	reqs   chan *block.Request
	err error

	jobs chan *blockJob
	workers uint32

	sessionOpts
}

func newSession(ctx context.Context, opts ...SessionOption) *Session {
	ctx, cancel := context.WithCancel(ctx)
	ses := &Session{
		reqs:   make(chan *block.Request, requestBufferSize),
		ctx:    ctx,
		cancel: cancel,
		jobs: make(chan *blockJob),
	}
	ses.parse(opts...)
	return ses
}

// Stream starts direct Block fetching from remote providers. It fetches the Blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also terminated with the provided context.
// Block order is guaranteed to be the same as requested through the `in` chan.
func (ses *Session) Stream(ctx context.Context, in <-chan []cid.Cid) (<-chan blocks.Block, <-chan error) {
	if ses.bs != nil {
		return ses.streamWithStore(ctx, in)
	}

	ctx, cancel := context.WithCancel(ctx)
	s := block.NewStream(ctx)

	go func() {
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
				if ses.err != nil {
					s.Error(ses.err)
				}

				cancel()
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return s.Blocks(), s.Errors()
}

// Blocks fetches Blocks by their CIDs evenly from the remote providers in the session.
// Block order is guaranteed to be the same as requested.
func (ses *Session) Blocks(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, <-chan error) {
	if len(ids) == 0 {
		ch := make(chan blocks.Block)
		close(ch)
		return ch, nil
	}

	if ses.bs != nil {
		return ses.blocksWithStore(ctx, ids)
	}

	ctx, cancel := context.WithCancel(ctx)
	s := block.NewStream(ctx)
	go func() {
		select {
		case <-ses.ctx.Done():
			if ses.err != nil {
				s.Error(ses.err)
			}

			cancel()
		case <-ctx.Done():
		}
	}()

	s.Enqueue(ses.request(ctx, ids)...)
	return s.Blocks(), s.Errors()
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

func (ses *Session) removeProvider() uint32 {
	return atomic.AddUint32(&ses.prvs, ^uint32(0))
}

func (ses *Session) getProviders() uint32 {
	return atomic.LoadUint32(&ses.prvs)
}

func (ses *Session) requestId() uint32 {
	return atomic.AddUint32(&ses.reqN, 1)
}

func (ses *Session) streamWithStore(ctx context.Context, in <-chan []cid.Cid) (<-chan blocks.Block, <-chan error) {
	outB, outErr := make(chan blocks.Block, len(in)), make(chan error, 1)
	go func() {
		defer close(outB)
		defer close(outErr)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		var j *blockJob
		head := make(chan *blockJob, 1)
		back := head

		for {
			select {
			case ids, ok := <-in:
				if !ok {
					close(back)
					in = nil
					continue
				}

				j = newJob(ids, back)
				if !ses.work(ctx, j) {
					return
				}

				back = j.next
			case j, ok := <-head:
				if !ok {
					return
				}

				if !j.sendResults(ctx, outB, outErr) {
					return
				}

				head = j.next
			case <-ses.ctx.Done():
				if ses.err != nil {
					select {
					case outErr <- ses.err:
					case <-ctx.Done():
						return
					}
				}

				return
			case <-ctx.Done():
			}
		}
	}()

	return outB, outErr
}

func (ses *Session) blocksWithStore(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, <-chan error) {
	outB, outErr := make(chan blocks.Block, len(ids)), make(chan error, 1)

	go func() {
		defer close(outB)
		defer close(outErr)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		done := make(chan *blockJob, 1)
		if !ses.work(ctx, newJob(ids, done)) {
			return
		}

		select {
		case j := <-done:
			if !j.sendResults(ctx, outB, outErr) {
				return
			}
		case <-ses.ctx.Done():
			if ses.err != nil {
				select {
				case outErr <- ses.err:
				case <-ctx.Done():
				}
			}

		case <-ctx.Done():
		}
	}()

	return outB, outErr
}

func (ses *Session) work(ctx context.Context, j *blockJob) bool {
	for {
		select {
		case ses.jobs <- j:
			return true
		case <-ctx.Done():
			return false
		default:
			ses.spawnWorker(ctx)
			select {
			case ses.jobs <- j:
				return true
			case <-ctx.Done():
				return false
			}
		}
	}
}

func (ses *Session) spawnWorker(ctx context.Context) {
	if atomic.AddUint32(&ses.workers, 1) >= maxAvailableWorkers {
		return
	}

	go ses.worker(ctx)
}

func (ses *Session) worker(ctx context.Context) {
	for {
		select {
		case j := <-ses.jobs:
			var fetch bool
			var fetched []blocks.Block
			var toFetch = make([]cid.Cid, len(j.bos))

			for i, bo := range j.bos {
				bo.Block, bo.Error = ses.bs.Get(bo.Id)
				if bo.Block == nil {
					toFetch[i] = bo.Id
					fetch = true
				}
			}

			if fetch {
				fetched = make([]blocks.Block, 0, len(toFetch))

				s := block.NewStream(ctx)
				s.Enqueue(ses.request(ctx,toFetch)...)

				for i, id := range toFetch { // ordering is guaranteed
					if !id.Defined() {
						continue
					}

					select {
					case b := <-s.Blocks():
						if id.Equals(b.Cid()) {
							j.bos[i].Block = b
						} else {
							for _, bo := range j.bos {
								if bo.Id.Equals(b.Cid()) {
									bo.Block = b
								}
							}
						}
						fetched = append(fetched, b)
					case err := <-s.Errors():
						j.bos[i].Error = err
					case <-ctx.Done():
						return
					}
				}
			}

			select {
			case j.done <- j:
				if len(fetched) > 0 && ses.save {
					err := ses.bs.PutMany(fetched)
					if err != nil {
						log.Errorf("Failed to save fetched blocks: %ses", err)
					}
				}
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

type blockJob struct {
	bos        []*block.Option
	next, done chan *blockJob
}

func newJob(ids []cid.Cid, done chan *blockJob) *blockJob {
	bos := make([]*block.Option, len(ids))
	for i, id := range ids {
		bos[i] = &block.Option{Id: id}
	}

	return &blockJob{
		bos:   bos,
		done:  done,
		next:  make(chan *blockJob, 1),
	}
}

func (j *blockJob) sendResults(ctx context.Context, outB chan blocks.Block, outErr chan error) bool {
	for _, bo := range j.bos {
		if bo.Block != nil {
			select {
			case outB <- bo.Block:
			case <-ctx.Done():
				return false
			}
			continue
		}

		if bo.Error == nil {
			bo.Error = blockstore.ErrNotFound
		}

		select {
		case outErr <- bo.Error:
		case <-ctx.Done():
			return false
		}
	}

	return true
}
