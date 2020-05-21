package blockstream

import (
	"context"
	"errors"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

var (
	errBufferOverflow = errors.New("blockstream: buffer overflow")
	errBufferClosed   = errors.New("blockstream: buffer closed")
)

// buffer is a dynamically sized Block buffer with a CID ordering done with linked list.
type buffer struct {
	closed uint32

	input, output chan blocks.Block
	blocks        map[cid.Cid]blocks.Block

	order *cidList
}

// NewBuffer creates new ordered Block buffer given size and limit.
func NewBuffer(ctx context.Context, size, limit int) *buffer {
	buf := &buffer{
		input:  make(chan blocks.Block, size/2),
		output: make(chan blocks.Block, size/4),
		blocks: make(map[cid.Cid]blocks.Block, size/4),
		order:  newList(limit),
	}
	go buf.fill(ctx)
	return buf
}

// Len returns shows amount of Block stored in the buffer.
func (buf *buffer) Len() int {
	return len(buf.blocks) + len(buf.input) + len(buf.output)
}

// Input returns channel to write Blocks into with unpredictable order.
// It is safe to write to the chan arbitrary amount of Blocks as the buffer is dynamic.
// Might be also used to close the buffer.
func (buf *buffer) Input() chan<- blocks.Block {
	return buf.input
}

// Output returns channel with Blocks ordered by an Order method.
func (buf *buffer) Output() <-chan blocks.Block {
	return buf.output
}

// Order adds CIDs as the order for blocks to be gotten with Output.
// It is required that Order is called first for Blocks' CIDs before their bodies are received from Input.
// Have to be called only from one goroutine.
func (buf *buffer) Order(ids ...cid.Cid) error {
	if buf.isClosed() {
		return errBufferClosed
	}

	return buf.order.Append(ids...)
}

// Close signals buffer to close.
// It may still work after to serve remaining Blocks.
// To terminate Buffer use context.
func (buf *buffer) Close() error {
	if buf.isClosed() {
		return errBufferClosed
	}

	buf.close()
	return nil
}

func (buf *buffer) fill(ctx context.Context) {
	var (
		toWrite blocks.Block
		output  chan<- blocks.Block
	)

	defer func() {
		close(buf.output)

		// because on input Blocks are not checked if they were ordered,
		// so there is chance to keep refs on Blocks that wont be output.
		for id := range buf.blocks {
			delete(buf.blocks, id)
		}

		if buf.order.Len() != 0 {
			log.Warnf("Buffer terminated with %d blocks remained unserved.", buf.order.Len())
		}

		buf.close()
		buf.order = nil // explicitly remove ref on the list.
	}()

	for {
		select {
		case bl, ok := <-buf.input:
			if !ok {
				if toWrite != nil || (buf.order.Len() > 0 && len(buf.blocks) != 0) { // if there is something to write
					buf.input = nil
					buf.close()
					continue
				}

				return
			}

			if toWrite == nil && buf.order.Len() > 0 && bl.Cid().Equals(buf.order.Back()) {
				toWrite = bl
				output = buf.output
				continue // no need to store in the map if it is a mach
			}

			buf.blocks[bl.Cid()] = bl // store received block in the map.
		case output <- toWrite: // if send succeed:
			delete(buf.blocks, buf.order.Pop())         // 1. remove the cid from order list; 2. remove block from map.
			if buf.order.Len() == 0 && buf.isClosed() { // 3. check maybe it is time to close the buf.
				return
			}

			// 4. block current select case till the 'toWrite' block is ready again.
			output, toWrite = nil, nil
		case <-ctx.Done():
			return
		}

		if buf.order.Len() > 0 && toWrite == nil { // if there is an ordered cid and there is no already block prepared.
			toWrite = buf.blocks[buf.order.Back()] // prepare a new one.
			if toWrite != nil {                    // if already in the map, allow writing to the output.
				output = buf.output
			}
		}
	}
}

func (buf *buffer) isClosed() bool {
	return atomic.LoadUint32(&buf.closed) == 1
}

func (buf *buffer) close() {
	atomic.CompareAndSwapUint32(&buf.closed, 0, 1)
}

// cidList is a lock-free linked list of cids.
type cidList struct {
	len, limit  uint32
	back, front *cidItem
}

// cidItem is an item of cidList that refs next item in a list.
type cidItem struct {
	cid  cid.Cid
	next *cidItem
}

// newList creates new limited cidList.
func newList(limit int) *cidList {
	itm := &cidItem{}
	return &cidList{limit: uint32(limit), back: itm, front: itm}
}

// Len returns length of the list.
func (l *cidList) Len() uint32 {
	return atomic.LoadUint32(&l.len)
}

// Back returns first cid in the list.
func (l *cidList) Back() cid.Cid {
	return l.back.cid
}

// Append adds given CIDs to the front.
// Must be called only from one goroutine.
func (l *cidList) Append(ids ...cid.Cid) error {
	ln := uint32(len(ids))
	if l.Len()+ln > l.limit {
		return errBufferOverflow
	}

	if !l.front.cid.Defined() {
		l.front.cid = ids[0]
		ids = ids[1:]
	}

	for _, id := range ids {
		if !id.Defined() {
			ln--
			continue
		}

		l.front.next = &cidItem{cid: id}
		l.front = l.front.next
	}

	atomic.AddUint32(&l.len, ln)
	return nil
}

// Pop removes and returns first cid from the list.
// Must be called only from one goroutine.
func (l *cidList) Pop() cid.Cid {
	id := l.back.cid
	if id.Defined() {
		if l.back.next != nil {
			l.back = l.back.next
		} else {
			l.back.cid = cid.Undef
		}

		atomic.AddUint32(&l.len, ^uint32(0))
	}

	return id
}
