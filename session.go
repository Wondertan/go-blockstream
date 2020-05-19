package blockstream

import (
	"context"
	"io"
	"sync"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

const (
	streamBufferSize  = 64
	streamBufferLimit = 1024
)

// tracker tracks blocks within the session
type tracker interface {
	putter
	getter
}

type Session struct {
	ctx    context.Context
	cancel context.CancelFunc

	rcvrs struct {
		s []*receiver
		l sync.RWMutex
	}

	trk tracker
}

func newSession(
	ctx context.Context,
	pg tracker,
	rws []io.ReadWriteCloser,
	t access.Token,
	onErr func(func() error),
) (ses *Session, err error) {
	ctx, cancel := context.WithCancel(ctx)
	ses = &Session{
		ctx:    ctx,
		cancel: cancel,
		rcvrs: struct {
			s []*receiver
			l sync.RWMutex
		}{},
		trk: pg,
	}

	ses.rcvrs.s = make([]*receiver, len(rws))
	for i, s := range rws {
		ses.rcvrs.s[i], err = newReceiver(ctx, pg, s, t, onErr)
		if err != nil {
			return
		}
	}

	return
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

				err := buf.Order(ids...)
				if err != nil {
					log.Error(err)
					return
				}

				err = ses.receive(ctx, ids, buf.Input())
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
	err := buf.Order(ids...)
	if err != nil {
		return nil, err
	}

	err = ses.receive(ctx, ids, buf.Input())
	if err != nil {
		return nil, err
	}

	return buf.Output(), buf.Close()
}

// Close ends session.
func (ses *Session) Close() error {
	ses.cancel()
	return nil
}

// receive requests providers in the session for ids and writes them to the chan.
func (ses *Session) receive(ctx context.Context, in []cid.Cid, out chan<- blocks.Block) error {
	in, err := ses.tracked(ctx, in, out)
	if len(in) == 0 || err != nil {
		return err
	}

	for prv, ids := range ses.distribute(in) {
		err = prv.receive(ctx, ids, out)
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
func (ses *Session) tracked(ctx context.Context, ids []cid.Cid, bs chan<- blocks.Block) ([]cid.Cid, error) {
	n := 0
	for _, id := range ids {
		if !id.Defined() {
			continue
		}

		b, err := ses.trk.Get(id)
		switch err {
		case blockstore.ErrNotFound:
			ids[n] = id
			n++
		case nil:
			select {
			case bs <- b:
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ses.ctx.Done():
				return nil, ctx.Err()
			}
		default:
			return nil, err
		}
	}

	return ids[:n], nil
}

func (ses *Session) addReceiver(prv *receiver) {
	ses.rcvrs.l.Lock()
	defer ses.rcvrs.l.Unlock()

	ses.rcvrs.s = append(ses.rcvrs.s, prv)
}
