package streaming

import (
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// getter is an interface responsible for getting blocks and their sizes.
type getter interface {
	GetSize(cid.Cid) (int, error)
	Get(cid.Cid) (blocks.Block, error)
}

// sender represents an entity responsible for sending blocks to remote peer which asked for them.
type sender struct {
	rwc    io.ReadWriteCloser
	blocks getter
	t      Token
	max    int
}

// newSender creates new sender.
func newSender(rwc io.ReadWriteCloser, blocks getter, max int, ot onToken, oe onClose) (*sender, error) {
	snr := &sender{rwc: rwc, blocks: blocks, max: max}
	err := snr.handleHandshake(ot)
	if err != nil {
		return nil, err
	}

	go oe(snr.readWrite)
	return snr, nil
}

func (snr *sender) handleHandshake(ot onToken) (err error) {
	snr.t, err = checkHand(snr.rwc, ot)
	return
}

// readWrite is a long running method which iteratively handles requests and responds to them.
func (snr *sender) readWrite() error {
	for {
		ids, err := readBlocksReq(snr.rwc)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return snr.rwc.Close()
			}

			return err
		}

		err = snr.send(ids)
		if err != nil {
			return err
		}
	}
}

// send iteratively writes bunches of blocks.
func (snr *sender) send(ids []cid.Cid) error {
	sent := 0
	toSend := len(ids)
	for {
		bs, err := snr.collect(ids[sent:])
		if err != nil {
			return err
		}

		err = writeBlocksResp(snr.rwc, bs)
		if err != nil {
			return err
		}

		sent += len(bs)
		if sent >= toSend {
			return nil
		}
	}
}

// collect aggregates blocks with cumulative size less than `max`.
func (snr *sender) collect(ids []cid.Cid) (bs []blocks.Block, err error) {
	var total int
	for _, id := range ids {
		size, err := snr.blocks.GetSize(id)
		if err != nil {
			return bs, fmt.Errorf("can't get size of requested block(%s): %w", id, err)
		}

		if size > snr.max {
			return bs, fmt.Errorf("found block bigger than max message size")
		}

		total += size
		if total > snr.max {
			return bs, err
		}

		b, err := snr.blocks.Get(id)
		if err != nil {
			return bs, fmt.Errorf("can't get requested block(%s): %w", id, err)
		}

		bs = append(bs, b)
	}

	return
}
