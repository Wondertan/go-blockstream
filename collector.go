package blockstream

import (
	"context"
	"fmt"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// blockGetter is an interface responsible for getting blocks and their sizes.
type blockGetter interface {
	GetSize(cid.Cid) (int, error)
	Get(cid.Cid) (blocks.Block, error)
	Has(cid.Cid) (bool, error)
}

// collector aggregates batches of blocks limited to some max size and fills requests with them.
type collector struct {
	max, total int

	reqs   <-chan *request
	blocks blockGetter

	ctx context.Context
}

// newCollector creates new collector
func newCollector(ctx context.Context, reqs <-chan *request, blocks blockGetter, max int, onErr onClose) *collector {
	c := &collector{
		max:    max,
		reqs:   reqs,
		blocks: blocks,
		ctx:    ctx,
	}
	go onErr(c.collect)
	return c
}

// collect waits for new requests and fulfills them.
func (c *collector) collect() error {
	for {
		select {
		case req := <-c.reqs:
			for {
				bs, err := c.getBlocks(req.Remains())
				if err != nil {
					return err
				}

				if !req.Fill(bs) {
					break
				}
			}
		case <-c.ctx.Done():
			return nil
		}
	}
}

// getBlocks reads up blocks by their ids but returns if max size limit is reached.
func (c *collector) getBlocks(ids []cid.Cid) (bs []blocks.Block, err error) {
	c.total = 0
	for _, id := range ids {
		size, err := c.blocks.GetSize(id)
		if err != nil {
			return bs, fmt.Errorf("can't get size of requested block(%s): %w", id, err)
		}

		if size > c.max {
			return bs, fmt.Errorf("found block bigger than limit message size")
		}

		c.total += size
		if c.total > c.max {
			return bs, nil
		}

		b, err := c.blocks.Get(id)
		if err != nil {
			return bs, fmt.Errorf("can't get requested block(%s): %w", id, err)
		}

		bs = append(bs, b)
	}

	return
}
