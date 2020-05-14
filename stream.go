package blockstream

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// TODO Ensure blocks are always received in the requested order.
type stream struct {
	ctx context.Context
	ses *Session

	in, out         chan blocks.Block
	buf             []blocks.Block
	ids             <-chan []cid.Cid
	remains         int
	bufLimit, limit int
}

func newStream(ctx context.Context, ids <-chan []cid.Cid, ses *Session, bufSize, bufLimit int) *stream {
	inSize, outSize := bufSize/2, bufSize/4
	b := &stream{
		ctx:      ctx,
		ses:      ses,
		in:       make(chan blocks.Block, inSize),
		out:      make(chan blocks.Block, outSize),
		buf:      make([]blocks.Block, 0, bufSize/4),
		ids:      ids,
		limit:    bufLimit,
		bufLimit: bufLimit - (inSize + outSize), // subtract what is buffered in channels
	}

	go b.stream()
	return b
}

func (s *stream) stream() {
	defer func() {
		close(s.out)
		if s.remains != 0 {
			log.Warnf("Stream finished with %d blocks remained unserved.", s.remains)
		}
	}()

	for {
		select {
		case bl := <-s.in:
			s.ses.track(bl)
			s.buf = append(s.buf, bl)
		case s.outCh() <- s.block():
			s.buf = s.buf[1:]
			s.remains--
			if s.remains == 0 && s.ids == nil {
				return
			}
		case ids, ok := <-s.ids:
			if !ok {
				if s.remains == 0 {
					return
				}

				s.ids = nil
				continue
			}

			if len(s.buf) == s.bufLimit { // not to leak the stream if reader is slow
				log.Warnf("Ignoring requested CIDs: stream buffer overflow(%d blocks).", s.limit)
				continue
			}

			err := s.ses.receive(s.ctx, ids, s)
			if err != nil {
				return
			}
		case <-s.ses.ctx.Done():
			return
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *stream) block() blocks.Block {
	if len(s.buf) > 0 {
		return s.buf[0]
	}

	return nil
}

func (s *stream) outCh() chan<- blocks.Block {
	if len(s.buf) > 0 {
		return s.out
	}

	return nil
}
