package blockstream

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// buffer is a an ordered dynamically sized Block buffer.
type buffer struct {
	in, out chan blocks.Block
	blocks  map[cid.Cid]blocks.Block
	order   []cid.Cid
	orderCh chan []cid.Cid
	limit   int
}

func newBuffer(ctx context.Context, bufSize, bufLimit int) *buffer {
	b := &buffer{
		in:      make(chan blocks.Block, bufSize/2),
		out:     make(chan blocks.Block, bufSize/4),
		blocks:  make(map[cid.Cid]blocks.Block, bufSize/4),
		order:   make([]cid.Cid, 0, bufSize),
		orderCh: make(chan []cid.Cid, 8),
		limit:   bufLimit,
	}
	go b.fill(ctx)
	return b
}

func (buf *buffer) Input() chan<- blocks.Block {
	return buf.in
}

func (buf *buffer) Output() <-chan blocks.Block {
	return buf.out
}

func (buf *buffer) Order() chan<- []cid.Cid {
	return buf.orderCh
}

func (buf *buffer) fill(ctx context.Context) {
	var (
		first blocks.Block
		out   chan<- blocks.Block
		write bool
	)

	defer func() {
		close(buf.out)
		if len(buf.order) != 0 {
			log.Warnf("Buffer closed with %d blocks remained unserved.", len(buf.order))
		}

		for id := range buf.blocks { // explicitly clean blocks.
			delete(buf.blocks, id)
		}
	}()

	for {
		if len(buf.order) > 0 && first == nil {
			first, write = buf.blocks[buf.order[0]]
			if write {
				out = buf.out
			}
		}

		select {
		case bl := <-buf.in:
			buf.blocks[bl.Cid()] = bl
		case out <- first:
			buf.order = buf.order[1:]
			if len(buf.order) == 0 && buf.orderCh == nil {
				return
			}

			out = nil
			first = nil
		case ids, ok := <-buf.orderCh:
			if !ok {
				if len(buf.order) == 0 {
					return
				}

				buf.orderCh = nil
				continue
			}

			if len(buf.order)+len(ids) == buf.limit+1 { // not to leak the fill if reader is slow
				log.Warnf("Ignoring requested CIDs: fill buffer overflow(%d blocks).", buf.limit)
				continue
			}

			buf.order = append(buf.order, ids...)
		case <-ctx.Done():
			return
		}
	}
}
