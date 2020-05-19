package blockstream

import (
	"context"
	"io"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
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
	ctx context.Context

	rcvrs []*receiver
	trk   tracker
}

func newSession(
	ctx context.Context,
	pg tracker,
	rws []io.ReadWriteCloser,
	t access.Token,
	onErr func(func() error),
) (ses *Session, err error) {
	ses = &Session{ctx: ctx, trk: pg}
	ses.rcvrs = make([]*receiver, len(rws))
	for i, s := range rws {
		ses.rcvrs[i], err = newReceiver(ctx, pg, s, t, onErr)
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
	l := len(ses.rcvrs)
	distrib := make(map[*receiver][]cid.Cid, l)
	for i, k := range ids {
		p := ses.rcvrs[i%l]
		distrib[p] = append(distrib[p], k)
	}

	return distrib
}

// tracked sends known Blocks to the chan and returns ids remained to be fetched.
func (ses *Session) tracked(ctx context.Context, ids []cid.Cid, bs chan<- blocks.Block) ([]cid.Cid, error) {
	var (
		n    int
		have []cid.Cid
	)
	for _, id := range ids {
		if !id.Defined() {
			continue
		}

		ok, err := ses.trk.Has(id)
		if err != nil {
			return nil, err
		}

		if ok {
			have = append(have, id) // TODO Reduce allocs
		} else {
			ids[n] = id
			n++
		}
	}

	go func() {
		for _, id := range have {
			b, err := ses.trk.Get(id)
			if err != nil {
				log.Errorf("Can't get tracked block: %s", err)
				continue
			}

			select {
			case bs <- b:
			case <-ctx.Done():
				return
			case <-ses.ctx.Done():
				return
			}
		}
	}()

	return ids[:n], nil
}
