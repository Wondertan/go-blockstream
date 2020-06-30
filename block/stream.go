package block

import (
	"context"
	"errors"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("blockstream")

var (
	errStreamClosed = errors.New("blockstream: stream closed")
)

// stream is a dynamically sized stream of Blocks with a strict CID ordering done with linked list.
type stream struct {
	closed uint32              // atomic states of closage.
	input  chan []blocks.Block // ingoing Blocks channel
	output chan blocks.Block   // outgoing Blocks channel.
	cache  Cache               // only accessed inside `stream()` routine, except for `Len()` method.
	queue  *cidQueue           // ordered list of CIDs
}

// NewStream creates new ordered Block stream given size and limit.
func NewStream(ctx context.Context, cache Cache, size int) *stream {
	buf := &stream{
		input:  make(chan []blocks.Block, size/4),
		output: make(chan blocks.Block, 3*size/4),
		cache:  cache,
		queue:  newCidQueue(),
	}
	go buf.stream(ctx)
	return buf
}

// Input returns channel to write Blocks into with unpredictable order.
// It is safe to write to the chan arbitrary amount of Blocks as the stream has dynamic cashing.
// Might be also used to close the stream.
func (buf *stream) Input() chan []blocks.Block { // TODO Change this to write only
	return buf.input
}

// Output returns channel with Blocks ordered by the Enqueue method.
func (buf *stream) Output() <-chan blocks.Block {
	return buf.output
}

// Enqueue adds CIDs as the order for blocks to be received with the Output.
// It is required that Enqueue is called first for Blocks' CIDs before they are actually received from the Input.
// Must be called only from one goroutine.
func (buf *stream) Enqueue(ids ...cid.Cid) error {
	if buf.isClosed() {
		return errStreamClosed
	}

	buf.queue.Enqueue(ids...)
	return nil
}

// Close signals stream to close.
// It may still work after to serve remaining Blocks.
// To terminate Buffer use context.
func (buf *stream) Close() error {
	if buf.isClosed() {
		return errStreamClosed
	}

	buf.close()
	return nil
}

func (buf *stream) stream(ctx context.Context) {
	var (
		pending cid.Cid           // first CID in a queue to be sent.
		toWrite blocks.Block      // Block to be written.
		output  chan blocks.Block // switching input and output channel to control select blocking.
		input   = buf.input
	)

	defer func() {
		l := buf.queue.Len()
		if l > 0 {
			log.Warnf("Buffer closed with %d Blocks remained enqueued, but unserved.", l)
		}

		close(buf.output)
		buf.queue = nil // explicitly remove ref on the list for GC to clean it.
	}()

	for {
		select {
		case bs, ok := <-input: // on received Block:
			if !ok { // if closed
				buf.close() // signal closing,
				if pending.Defined() {
					input = nil // block the current case,
					continue    // and continue writing.
				}

				return // or stop.
			}

			for _, b := range bs { // iterate over received blocks
				buf.cache.Add(b)             // and cache them.
				if b.Cid().Equals(pending) { // if it is a match,
					toWrite, output = b, buf.output // write the Block,
				}
			}
		case output <- toWrite: // on sent Block:
			if buf.queue.Len() == 0 && buf.isClosed() { // check maybe it is time to close the stream,
				return
			}

			output, toWrite, pending = nil, nil, cid.Undef // or block current select case and clean sent data.
		case <-ctx.Done():
			buf.close()
			return
		}

		if buf.queue.Len() > 0 && !pending.Defined() { // if there is something in a queue and no pending,
			pending = buf.queue.Dequeue() // define newer pending,
		}

		if toWrite == nil && pending.Defined() { // if we don't have the pending Block,
			toWrite = buf.cache.Get(pending) // try to get it from the cache,
			if toWrite != nil {              // and on success
				output = buf.output // unblock output
			}
		}
	}
}

func (buf *stream) isClosed() bool {
	return atomic.LoadUint32(&buf.closed) == 1
}

func (buf *stream) close() {
	atomic.CompareAndSwapUint32(&buf.closed, 0, 1)
}
