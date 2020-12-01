package blockstream

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/Wondertan/go-blockstream/block"
)

const (
	maxAvailableWorkers = 128
	requestBufferSize   = 8
)

// TODO Refactor this, my ayes hurt watching this
type Session struct {
	ctx    context.Context
	cancel context.CancelFunc

	reqN, prvs uint32
	reqs       chan *block.Request
	err        error

	jobch chan *blockJob

	workers, jobs uint32

	sessionOpts
}

func newSession(ctx context.Context, opts ...SessionOption) *Session {
	ctx, cancel := context.WithCancel(ctx)
	ses := &Session{
		reqs:   make(chan *block.Request, requestBufferSize),
		ctx:    ctx,
		cancel: cancel,
		jobch:  make(chan *blockJob),
	}
	ses.parse(opts...)
	return ses
}

// Stream starts direct Block fetching from remote providers. It fetches the Blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also terminated with the provided context.
// Block order is guaranteed to be the same as requested through the `in` chan.
func (ses *Session) Stream(ctx context.Context, in <-chan []cid.Cid) (<-chan block.Result, <-chan error) {
	if ses.bs != nil {
		return ses.streamWithStore(ctx, in)
	}

	stream, errOut := block.NewStream(ctx), make(chan error, 1)
	go func() {
		defer close(errOut)
		for {
			select {
			case ids, ok := <-in:
				if !ok {
					stream.Close()
					in = nil
					continue
				}

				stream.Enqueue(ses.request(ctx, ids)...)
			case <-ses.ctx.Done():
				if ses.err != nil {
					errOut <- ses.err
				}

				return
			case <-stream.Done():
				return
			}
		}
	}()

	return stream.Output(), errOut
}

// Blocks fetches Blocks by their CIDs evenly from the remote providers in the session.
// Block order is guaranteed to be the same as requested.
func (ses *Session) Blocks(ctx context.Context, ids []cid.Cid) (<-chan block.Result, <-chan error) {
	if len(ids) == 0 {
		ch := make(chan block.Result)
		close(ch)
		return ch, nil
	}

	if ses.bs != nil {
		return ses.blocksWithStore(ctx, ids)
	}

	stream, errOut := block.NewStream(ctx), make(chan error, 1)
	stream.Enqueue(ses.request(ctx, ids)...)
	stream.Close()

	go func() {
		defer close(errOut)
		select {
		case <-ses.ctx.Done():
			if ses.err != nil {
				errOut <- ses.err
			}
		case <-stream.Done():
		}
	}()

	return stream.Output(), errOut
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
	filtered := make([]cid.Cid, 0, len(ids))
	for _, id := range ids {
		if !id.Defined() {
			continue
		}
		filtered = append(filtered, id)
	}

	prs, l := int(atomic.LoadUint32(&ses.prvs)), len(filtered)
	sets := make([][]cid.Cid, prs)
	for i := 0; i < prs; i++ {
		sets[i] = filtered[i*l/prs : (i+1)*l/prs]
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

func (ses *Session) streamWithStore(ctx context.Context, in <-chan []cid.Cid) (<-chan block.Result, <-chan error) {
	ctx, cancel := context.WithCancel(ctx)
	outR, outErr := make(chan block.Result, len(in)), make(chan error, 1)
	first := make(chan *blockJob, 1)

	go func() { // handles input
		last := first
		for {
			select {
			case ids, ok := <-in:
				if !ok {
					close(last)
					return
				}
				last = ses.process(ctx, ids, last)
			case <-ses.ctx.Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() { // handles output
		defer func() {
			cancel()
			close(outR)
			close(outErr)
		}()

		for {
			select {
			case j, ok := <-first:
				if !ok {
					return
				}

				j.write(outR)
				first = j.next
			case <-ses.ctx.Done():
				if ses.err != nil {
					select {
					case outErr <- ses.err:
					case <-ctx.Done():
					}
				}
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	return outR, outErr
}

func (ses *Session) blocksWithStore(ctx context.Context, ids []cid.Cid) (<-chan block.Result, <-chan error) {
	ctx, cancel := context.WithCancel(ctx)
	outR, outErr := make(chan block.Result, len(ids)), make(chan error, 1)
	done := make(chan *blockJob, 1)

	go func() {
		defer func() {
			cancel()
			close(outR)
			close(outErr)
		}()

		ses.process(ctx, ids, done)
		select {
		case j := <-done:
			j.write(outR)
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

	return outR, outErr
}

func (ses *Session) process(ctx context.Context, ids []cid.Cid, done chan *blockJob) chan *blockJob {
	last := make(chan *blockJob, 1)
	j := ses.newJob(ctx, ids, done, last)
	select {
	case ses.jobch <- j:
	case <-ses.ctx.Done():
	default:
		ses.spawnWorker()
		select {
		case ses.jobch <- j:
		case <-ses.ctx.Done():
		}
	}

	return last
}

func (ses *Session) spawnWorker() {
	id := atomic.AddUint32(&ses.workers, 1)
	if id >= maxAvailableWorkers {
		return
	}

	log.Debugf("New Worker %d spawned.", id)
	go ses.worker(id)
}

func (ses *Session) worker(id uint32) {
	for {
		select {
		case j := <-ses.jobch:
			log.Debugf("Worker %d started processing Job %d.", id, j.id)

			var fetch bool
			var fetched []blocks.Block
			toFetch := make([]cid.Cid, len(j.results))

			for i, res := range j.results {
				res.Block, res.Error = ses.bs.Get(res.Cid)
				if res.Error != nil {
					toFetch[i] = res.Cid
					fetch = true
				} else {
					continue
				}
			}

			if fetch && !ses.offline {
				log.Debugf("Fetching for Job %d started", j.id)

				// FIXME Work with requests directly
				s := block.NewStream(j.ctx)
				s.Enqueue(ses.request(j.ctx, toFetch)...)
				s.Close()

				fetched = make([]blocks.Block, 0, len(toFetch))
				for i, id := range toFetch {
					if !id.Defined() {
						continue
					}

					select {
					case res := <-s.Output():
						*j.results[i] = res
						if res.Block != nil {
							fetched = append(fetched, res.Block)
						}
					case <-j.ctx.Done():
						j.results[i].Error = j.ctx.Err()
					}
				}

				log.Debugf("Fetching for Job %d finished", j.id)
			}

			select {
			case j.done <- j:
				if len(fetched) > 0 && ses.save {
					err := ses.bs.PutMany(fetched)
					if err != nil {
						log.Errorf("Failed to save fetched blocks: %ses", err)
					}
				}
			case <-j.ctx.Done():
			}
		case <-ses.ctx.Done():
			return
		}
	}
}

type blockJob struct {
	id         uint32
	ctx        context.Context
	results    []*block.Result
	next, done chan *blockJob
}

func (ses *Session) newJob(ctx context.Context, ids []cid.Cid, done, next chan *blockJob) *blockJob {
	results := make([]*block.Result, len(ids))
	for i, id := range ids {
		results[i] = &block.Result{Cid: id}
	}

	j := &blockJob{
		id:      atomic.AddUint32(&ses.jobs, 1),
		ctx:     ctx,
		results: results,
		done:    done,
		next:    next,
	}

	log.Debugf("Got new Job %d.", j.id)
	return j
}

func (j *blockJob) write(outR chan block.Result) {
	for _, res := range j.results {
		select {
		case outR <- *res:
		case <-j.ctx.Done():
		}
	}

	log.Debugf("Job %d was processed.", j.id)
}
