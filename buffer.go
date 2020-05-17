package blockstream

import (
	"context"
	"errors"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var (
	errBufferOverflow = errors.New("buffer overflow")
	errBufferClosed   = errors.New("buffer closed")
)

type item struct {
	cid  cid.Cid
	next *item
}

// buffer is a dynamically sized Block buffer with a CID ordering done with linked list.
type buffer struct {
	len, limit, closed uint32

	input, output chan blocks.Block
	blocks        map[cid.Cid]blocks.Block
	first, last   *item
}

func newBuffer(ctx context.Context, bufSize, bufLimit int) *buffer {
	buf := &buffer{
		input:  make(chan blocks.Block, bufSize/2),
		output: make(chan blocks.Block, bufSize/4),
		blocks: make(map[cid.Cid]blocks.Block, bufSize/4),
		limit:  uint32(bufLimit),
	}
	go buf.fill(ctx)
	return buf
}

func (buf *buffer) Input() chan<- blocks.Block {
	return buf.input
}

func (buf *buffer) Output() <-chan blocks.Block {
	return buf.output
}

func (buf *buffer) Order(ids ...cid.Cid) error {
	if atomic.LoadUint32(&buf.closed) == 1 {
		return errBufferClosed
	}

	l := uint32(len(ids))
	if atomic.LoadUint32(&buf.len)+l == buf.limit+1 {
		return errBufferOverflow
	}

	for _, id := range ids {
		item := &item{cid: id}
		if buf.last == nil {
			buf.last, buf.first = item, item
		} else {
			buf.last.next = item
			buf.last = item
		}
	}

	atomic.AddUint32(&buf.len, l)
	return nil
}

// Close signals buffer to close.
// It may still work after to serve remaining Blocks, to terminate Buffer use context.
func (buf *buffer) Close() error {
	if atomic.LoadUint32(&buf.closed) == 1 {
		return errBufferClosed
	}

	atomic.StoreUint32(&buf.closed, 1)
	return nil
}

func (buf *buffer) fill(ctx context.Context) {
	var (
		first  blocks.Block
		output chan<- blocks.Block
		write  bool
	)

	defer func() {
		buf.first, buf.last = nil, nil
		close(buf.output)
		for id := range buf.blocks { // explicitly clean blocks.
			delete(buf.blocks, id)
		}

		atomic.CompareAndSwapUint32(&buf.closed, 0, 1) // set as closed, if terminated with context.

		if atomic.LoadUint32(&buf.len) != 0 {
			log.Warnf("Buffer terminated with %d blocks remained unserved.", atomic.LoadUint32(&buf.len))
		}
	}()

	for {
		select {
		case bl := <-buf.input:
			buf.blocks[bl.Cid()] = bl
		case output <- first:
			id := buf.first.cid
			buf.first = buf.first.next
			delete(buf.blocks, id)

			atomic.AddUint32(&buf.len, ^uint32(0))
			if atomic.LoadUint32(&buf.len) == 0 && atomic.LoadUint32(&buf.closed) == 1 {
				return
			}

			output = nil
			first = nil
		case <-ctx.Done():
			return
		}

		if atomic.LoadUint32(&buf.len) > 0 && first == nil {
			first, write = buf.blocks[buf.first.cid]
			if write {
				output = buf.output
			}
		}
	}
}
