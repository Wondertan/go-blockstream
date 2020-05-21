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

// buffer is a dynamically sized Block buffer with a strict CID ordering done with linked list.
type buffer struct {
	len, closed uint32 // atomic states of length and closage.

	input, output chan blocks.Block        // read, write channels for in/outcoming Blocks.
	blocks        map[cid.Cid]blocks.Block // only accessed inside `buffer()` routine, except for `Len()` method.

	queue *cidList
}

// NewBuffer creates new ordered Block buffer given size and limit.
func NewBuffer(ctx context.Context, size, limit int) *buffer {
	buf := &buffer{
		input:  make(chan blocks.Block, size/2),
		output: make(chan blocks.Block, size/4),
		blocks: make(map[cid.Cid]blocks.Block, (size/4)-1), // decremented because of the `toWrite` slot in the `buffer()`
		queue:  newList(limit),
	}
	go buf.buffer(ctx)
	return buf
}

// Len returns the amount of all Blocks stored in the buffer.
func (buf *buffer) Len() int {
	return int(atomic.LoadUint32(&buf.len)) + len(buf.input) + len(buf.output)
}

// Input returns channel to write Blocks into with unpredictable queue.
// It is safe to write to the chan arbitrary amount of Blocks as the buffer is dynamic.
// Might be also used to close the buffer.
func (buf *buffer) Input() chan<- blocks.Block {
	return buf.input
}

// Output returns channel with Blocks ordered by an Enqueue method.
func (buf *buffer) Output() <-chan blocks.Block {
	return buf.output
}

// Enqueue adds CIDs as the order for blocks to be received with the Output.
// It is required that Enqueue is called first for Blocks' CIDs before they are actually received from the Input.
// Must be called only from one goroutine.
func (buf *buffer) Enqueue(ids ...cid.Cid) error {
	if buf.isClosed() {
		return errBufferClosed
	}

	return buf.queue.Append(ids...)
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

// buffer does actual buffering magic.
func (buf *buffer) buffer(ctx context.Context) {
	var (
		pending cid.Cid             // first CID in a queue to be sent.
		toWrite blocks.Block        // Block to be written.
		output  chan<- blocks.Block // switching output channel to control select blocking.
	)

	defer func() {
		l := atomic.LoadUint32(&buf.len)
		if l > 0 {
			log.Warnf("Buffer closed with %d Blocks remaining.", l)
		}

		l = buf.queue.Len()
		if l > 0 {
			log.Warnf("Buffer closed with %d Blocks remained enqueued, but unserved.", l)
		}

		close(buf.output)
		for id := range buf.blocks { // Blocks are not checked regarding their persistence in a queue,
			delete(buf.blocks, id) // so there is chance to get Blocks that wont be output and leak.
		}
		buf.queue = nil // explicitly remove ref on the list for GC to clean it.
	}()

	for {
		select {
		case b, ok := <-buf.input: // on received Block:
			if !ok { // if closed
				buf.close()                          // signal closing,
				if atomic.LoadUint32(&buf.len) > 0 { // if there is something to write,
					buf.input = nil // block the current case,
					continue        // and continue writing.
				}

				return // or stop.
			}

			atomic.AddUint32(&buf.len, 1) // increment internal buffer length.
			if b.Cid().Equals(pending) {  // if it is a match,
				toWrite, output = b, buf.output // write the Block,
				continue
			}

			buf.blocks[b.Cid()] = b // or store received block in the map.
		case output <- toWrite: // on sent Block:
			atomic.AddUint32(&buf.len, ^uint32(0))      // decrement internal buffer length,
			if buf.queue.Len() == 0 && buf.isClosed() { // check maybe it is time to close the buf,
				return
			}

			output, toWrite, pending = nil, nil, cid.Undef // or block current select case and clean sent data.
		case <-ctx.Done():
			buf.close()
			return
		}

		if buf.queue.Len() > 0 && !pending.Defined() { // if there is something in a queue and no pending,
			pending = buf.queue.Pop() // define newer pending,
		}

		if toWrite == nil { // if we don't have the pending Block,
			toWrite = buf.blocks[pending] // try to get it from the map,
			if toWrite != nil {           // and on success
				output = buf.output         // unblock output
				delete(buf.blocks, pending) // and remove it from the map.
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
