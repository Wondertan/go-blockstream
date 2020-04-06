package blockstream

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const getTimeout = time.Minute * 15

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

func (f *Session) GetBlocks(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, error) {
	out := make(chan blocks.Block)
	if len(ids) == 0 {
		close(out)
		return out, nil
	}

	go func() {
		defer close(out)

		ctx, cancel := context.WithTimeout(ctx, getTimeout)
		defer cancel()

		remains, err := f.getBlocks(ctx, ids, out)
		if err != nil {
			log.Error("GetBlocks finished earlier with %d remaining blocks.", len(remains))
			log.Error(err)
		}
	}()

	return out, nil
}

// receive fetches blocks by ids.
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
