package blockstream

import (
	"context"
	"io"
	"sync"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const (
	streamBufferSize  = 32
	streamBufferLimit = 1024
)

// TODO Manage state with goroutine.
// TODO Refactor tracking.
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
// Block order is not guaranteed in case of multiple providers.
// Does not request blocks if they are already requested/received.
func (ses *Session) Stream(ctx context.Context, idch <-chan []cid.Cid) <-chan blocks.Block {
	buf := newBuffer(ctx, streamBufferSize, streamBufferLimit)
	go func() {
		defer buf.Close()
		for {
			select {
			case ids, ok := <-idch:
				if !ok {
					return
				}

				err := buf.Order(ids...)
				if err != nil {
					return
				}

				err = ses.receive(ctx, ids, buf.Input())
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return buf.Output()
}

// Blocks fetches blocks by their ids from the providers in the session.
// Order is not guaranteed.
func (ses *Session) Blocks(ctx context.Context, ids []cid.Cid) <-chan blocks.Block {
	buf := newBuffer(ctx, len(ids), len(ids))
	buf.Order(ids...) // won't error

	err := ses.receive(ctx, ids, buf.Input())
	if err != nil {
		return nil
	}

	buf.Close() // won't error
	return buf.Output()
}

func (ses *Session) Close() error {
	ses.cancel()
	for id := range ses.blocks.m { // explicitly clean the tracked blocks.
		delete(ses.blocks.m, id)
	}

	return nil
}

// receive requests providers in the session for ids and writes them to the chan.
func (ses *Session) receive(ctx context.Context, ids []cid.Cid, bs chan<- blocks.Block) error {
	ids, err := ses.tracked(ctx, ids, bs) // TODO Prevent blocks being requested twice in all cases.
	if len(ids) == 0 || err != nil {
		return err
	}

	for prv, ids := range ses.distribute(ids) {
		err = prv.receive(ctx, ids, bs)
		if err != nil {
			return err
		}
	}

	return nil
}

// distribute splits ids between providers to download from multiple sources.
func (ses *Session) distribute(ids []cid.Cid) map[*receiver][]cid.Cid {
	ses.rcvrs.l.RLock()
	defer ses.rcvrs.l.RUnlock()

	l := len(ses.rcvrs.s)
	distrib := make(map[*receiver][]cid.Cid, l)
	for i, k := range ids {
		p := ses.rcvrs.s[i%l]
		distrib[p] = append(distrib[p], k)
	}

	return distrib
}

// tracked fills buffer with tracked blocks and returns ids remained to be fetched.
func (ses *Session) tracked(ctx context.Context, in []cid.Cid, bs chan<- blocks.Block) (out []cid.Cid, err error) {
	ses.blocks.l.RLock()
	defer ses.blocks.l.RUnlock()

	for _, id := range in {
		if !id.Defined() {
			continue
		}

		b, ok := ses.blocks.m[id]
		if ok {
			select {
			case bs <- b:
			case <-ctx.Done():
				return out, ctx.Err()
			}
		} else {
			out = append(out, id)
		}
	}

	return
}

func (ses *Session) addReceiver(prv *receiver) {
	ses.rcvrs.l.Lock()
	defer ses.rcvrs.l.Unlock()

	ses.rcvrs.s = append(ses.rcvrs.s, prv)
}

func (ses *Session) track(b blocks.Block) {
	ses.blocks.l.Lock()
	defer ses.blocks.l.Unlock()

	ses.blocks.m[b.Cid()] = b
}
