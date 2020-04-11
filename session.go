package blockstream

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const timeout = time.Minute * 15

type Session struct {
	ctx  context.Context
	prvs []*receiver
	l    sync.RWMutex
}

func newSession(
	ctx context.Context,
	put putter,
	rws []io.ReadWriteCloser,
	t Token,
	onErr func(func() error),
) (_ *Session, err error) {
	prvs := make([]*receiver, len(rws))
	for i, s := range rws {
		prvs[i], err = newReceiver(ctx, put, s, t, onErr)
		if err != nil {
			return
		}
	}

	return &Session{ctx: ctx, prvs: prvs}, nil
}

// Stream starts direct block fetching from remote providers. It fetches the blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also stopped by force with the provided context.
func (f *Session) Stream(ctx context.Context, in chan []cid.Cid) <-chan blocks.Block {
	remains := cid.NewSet()
	out := make(chan blocks.Block)

	go func() {
		defer func() {
			close(out)
			if remains.Len() != 0 {
				log.Warnf("Stream finished with %d blocks remained unloaded.", remains.Len())
			}
		}()

		buf := make(chan blocks.Block, 8)
		for {
			select {
			case b := <-buf:
				select {
				case out <- b:
					// TODO Track metrics
					remains.Remove(b.Cid())
					if remains.Len() == 0 && in == nil {
						return
					}
				case <-ctx.Done(): // Only care about Stream context here.
					return
				}
			case ids, ok := <-in:
				if !ok {
					if remains.Len() > 0 {
						in = nil
						continue
					} else {
						return
					}
				}

				for _, id := range ids {
					remains.Add(id)
				}

				for prv, ids := range f.distribute(ids) {
					err := prv.receive(ctx, ids, buf)
					if err != nil {
						return
					}
				}
			case <-ctx.Done():
				return
			case <-f.ctx.Done():
				return
			}
		}
	}()

	return out
}

// Blocks fetches blocks by their ids from the providers in the session.
func (f *Session) Blocks(ctx context.Context, ids []cid.Cid) <-chan blocks.Block {
	out := make(chan blocks.Block)
	if len(ids) == 0 {
		close(out)
		return out
	}

	go func() {
		defer close(out)

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		remains, err := f.getBlocks(ctx, ids, out)
		if err != nil {
			log.Warnf("Blocks finished earlier with %d remaining blocks.", len(remains))
		}
	}()

	return out
}

// On error returns the error itself with remaining ids which were not received.
func (f *Session) getBlocks(ctx context.Context, ids []cid.Cid, out chan<- blocks.Block) ([]cid.Cid, error) {
	in := make(chan blocks.Block, len(ids))
	for prv, ids := range f.distribute(ids) {
		err := prv.receive(ctx, ids, in)
		if err != nil {
			return ids, err
		}
	}

	remains := cid.NewSet()
	for _, id := range ids {
		remains.Add(id)
	}

	for {
		select {
		case b := <-in:
			select {
			case out <- b:
				remains.Remove(b.Cid())
				if remains.Len() == 0 {
					return nil, nil
				}
			case <-ctx.Done(): // GetBlocks context
				return remains.Keys(), ctx.Err()
			}
		case <-f.ctx.Done(): // Session context
			return remains.Keys(), f.ctx.Err()
		}
	}
}

// distribute splits ids between providers to download from multiple sources.
func (f *Session) distribute(ids []cid.Cid) map[*receiver][]cid.Cid {
	f.l.RLock()
	defer f.l.RUnlock()

	l := len(f.prvs)
	distrib := make(map[*receiver][]cid.Cid, l)
	for i, k := range ids {
		p := f.prvs[i%l]
		distrib[p] = append(distrib[p], k)
	}

	return distrib
}

func (f *Session) addReceiver(prv *receiver) {
	f.l.Lock()
	defer f.l.Unlock()

	f.prvs = append(f.prvs, prv)
}
