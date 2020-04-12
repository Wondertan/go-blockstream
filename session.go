package blockstream

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const timeout = time.Minute * 15

type Session struct {
	ctx    context.Context
	cancel context.CancelFunc

	blocks struct {
		m map[cid.Cid]blocks.Block
		l sync.RWMutex
	}

	rcvrs struct {
		s []*receiver
		l sync.RWMutex
	}
}

func newSession(
	ctx context.Context,
	put putter,
	rws []io.ReadWriteCloser,
	t access.Token,
	onErr func(func() error),
) (_ *Session, err error) {
	ctx, cancel := context.WithCancel(ctx)
	rcvrs := make([]*receiver, len(rws))
	for i, s := range rws {
		rcvrs[i], err = newReceiver(ctx, put, s, t, onErr)
		if err != nil {
			return
		}
	}

	return &Session{
		ctx:    ctx,
		cancel: cancel,
		rcvrs: struct {
			s []*receiver
			l sync.RWMutex
		}{s: rcvrs},
		blocks: struct {
			m map[cid.Cid]blocks.Block
			l sync.RWMutex
		}{m: make(map[cid.Cid]blocks.Block)},
	}, nil
}

// Stream starts direct block fetching from remote providers. It fetches the blocks requested with 'in' chan by their ids.
// Stream is automatically stopped when both: the requested blocks are all fetched and 'in' chan is closed.
// It might be also stopped by force with the provided context.
// Order is not guaranteed.
func (f *Session) Stream(ctx context.Context, in <-chan []cid.Cid) <-chan blocks.Block {
	remains := 0
	out := make(chan blocks.Block)

	go func() {
		defer func() {
			close(out)
			if remains != 0 {
				log.Warnf("Stream finished with %d blocks remained unloaded.", remains)
			}
		}()

		buf := make(chan blocks.Block, 8)
		for {
			select {
			case b := <-buf:
				f.trackBlock(b)
				select {
				case out <- b:
					remains--
					if remains == 0 && in == nil {
						return
					}
				case <-ctx.Done(): // Only care about Stream context here.
					return
				}
			case ids, ok := <-in:
				if !ok {
					if remains != 0 {
						in = nil
						continue
					} else {
						return
					}
				}

				trkd, ids := f.tracked(ids)
				for prv, ids := range f.distribute(ids) {
					err := prv.receive(ctx, ids, buf)
					if err != nil {
						return
					}
				}

				for _, b := range trkd {
					select {
					case out <- b:
					case <-ctx.Done():
						return
					}
				}

				remains += len(ids)
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
	trkd, ids := f.tracked(ids)
	remains := len(ids)
	out := make(chan blocks.Block)
	if remains == 0 {
		close(out)
		return out
	}

	go func() {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer func() {
			cancel()
			close(out)
			if remains != 0 {
				log.Warnf("Blocks finished earlier with %d remaining blocks.", remains)
			}
		}()

		buf := make(chan blocks.Block, len(ids))
		for prv, ids := range f.distribute(ids) {
			err := prv.receive(ctx, ids, buf)
			if err != nil {
				return
			}
		}

		for _, b := range trkd {
			select {
			case out <- b:
			case <-ctx.Done():
				return
			}
		}

		for {
			select {
			case b := <-buf:
				f.trackBlock(b)
				select {
				case out <- b:
					remains--
					if remains == 0 {
						return
					}
				case <-ctx.Done(): // GetBlocks context
					return
				}
			case <-f.ctx.Done(): // Session context
				return
			}
		}
	}()

	return out
}

func (f *Session) Close() error {
	f.cancel()
	for id := range f.blocks.m { // explicitly clean the tracked blocks.
		delete(f.blocks.m, id)
	}

	return nil
}

// distribute splits ids between providers to download from multiple sources.
func (f *Session) distribute(ids []cid.Cid) map[*receiver][]cid.Cid {
	f.rcvrs.l.RLock()
	defer f.rcvrs.l.RUnlock()

	l := len(f.rcvrs.s)
	distrib := make(map[*receiver][]cid.Cid, l)
	for i, k := range ids {
		p := f.rcvrs.s[i%l]
		distrib[p] = append(distrib[p], k)
	}

	return distrib
}

func (f *Session) addReceiver(prv *receiver) {
	f.rcvrs.l.Lock()
	defer f.rcvrs.l.Unlock()

	f.rcvrs.s = append(f.rcvrs.s, prv)
}

func (f *Session) trackBlock(b blocks.Block) {
	f.blocks.l.Lock()
	defer f.blocks.l.Unlock()

	f.blocks.m[b.Cid()] = b
}

func (f *Session) tracked(in []cid.Cid) (bs []blocks.Block, out []cid.Cid) {
	f.blocks.l.RLock()
	defer f.blocks.l.RUnlock()

	for _, id := range in {
		b, ok := f.blocks.m[id]
		if ok {
			bs = append(bs, b)
		} else {
			out = append(out, id)
		}
	}

	return
}
