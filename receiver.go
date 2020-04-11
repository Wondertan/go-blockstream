package blockstream

import (
	"context"
	"io"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

const queueSize = 8

// putter is an interface responsible for saving blocks.
type putter interface {
	PutMany([]blocks.Block) error
}

type nilPutter struct{}

func (f *nilPutter) PutMany([]blocks.Block) error {
	return nil
}

// receiver represents an entity responsible for retrieving blocks from remote peer.
type receiver struct {
	put putter
	rwc io.ReadWriteCloser
	t   Token

	ctx     context.Context
	writeCh chan *write
	readCh  chan *read
}

// newReceiver creates new receiver from fakeStream.
func newReceiver(
	ctx context.Context,
	put putter,
	rwc io.ReadWriteCloser,
	t Token,
	onErr func(func() error),
) (*receiver, error) {
	rcv := &receiver{
		put:     put,
		rwc:     rwc,
		t:       t,
		ctx:     ctx,
		writeCh: make(chan *write, queueSize),
		readCh:  make(chan *read, queueSize),
	}
	if rcv.put == nil {
		rcv.put = &nilPutter{}
	}

	err := rcv.handleHandshake(t)
	if err != nil {
		return nil, err
	}

	go onErr(rcv.write)
	go onErr(rcv.read)

	return rcv, nil
}

// receive retrieves blocks by their ids from the remote sender and sends them to the channel in original order.
func (rcv *receiver) receive(ctx context.Context, ids []cid.Cid, out chan<- blocks.Block) error {
	// TODO Guarantee that write and read are always sent to the chans together.
	select {
	case rcv.writeCh <- &write{ids: ids}:
	case <-rcv.ctx.Done():
		return rcv.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case rcv.readCh <- &read{ctx: ctx, ids: ids, out: out}:
	case <-rcv.ctx.Done():
		return rcv.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// write is a long running method which handles writes to the receiver.
// it is done in a separate from reads routine not to block request writing.
func (rcv *receiver) write() error {
	for {
		select {
		case w := <-rcv.writeCh:
			err := rcv.handleWrite(w)
			if err != nil {
				return err
			}
		case <-rcv.ctx.Done():
			return rcv.handleCloseWrite()
		}
	}
}

// read is a long running method which handles blocks from the receiver.
func (rcv *receiver) read() error {
	for {
		select {
		case r := <-rcv.readCh:
			err := rcv.handleRead(r)
			if err != nil {
				return err
			}
		case <-rcv.ctx.Done():
			return rcv.handleCloseRead()
		}
	}
}

func (rcv *receiver) handleHandshake(t Token) error {
	return giveHand(rcv.rwc, t)
}

func (rcv *receiver) handleCloseWrite() error {
	return rcv.rwc.Close()
}

func (rcv *receiver) handleCloseRead() error {
	_, err := rcv.rwc.Read([]byte{0})
	if err != io.EOF {
		return err
	}

	return nil
}

// write is a tuple of params needed for writing block request.
type write struct {
	ids []cid.Cid
}

// handleWrite sends request for blocks through the fakeStream.
func (rcv *receiver) handleWrite(w *write) error {
	return writeBlocksReq(rcv.rwc, w.ids)
}

// read is a tuple of params needed for reading blocks.
type read struct {
	ctx context.Context
	ids []cid.Cid
	out chan<- blocks.Block
}

// handleRead iteratively reads requested blocks from the fakeStream and sends them to out channel.
func (rcv *receiver) handleRead(r *read) error {
	received := 0
	expected := len(r.ids)
	for {
		bs, err := readBlocksResp(rcv.rwc, r.ids[received:])
		if err != nil {
			return err
		}

		err = rcv.put.PutMany(bs)
		if err != nil {
			return err
		}

		for _, b := range bs {
			select {
			case r.out <- b:
			case <-r.ctx.Done():
				// it is still required to read the whole request out, even the context is canceled.
			}
		}

		received += len(bs)
		if received == expected {
			return nil
		}
	}
}
