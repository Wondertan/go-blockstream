package block

// // Collector aggregates batches of blocks limited to some max size and fills requests with them.
// type Collector struct {
// 	max, total int
//
// 	reqs   <-chan *Request
// 	blocks blockstore.Blockstore
//
// 	ctx context.Context
// }
//
// // NewCollector creates new Collector
// func NewCollector(ctx context.Context, reqs <-chan *Request, blocks blockstore.Blockstore, max int) *Collector {
// 	c := &Collector{max: max, reqs: reqs, blocks: blocks, ctx: ctx}
// 	go c.collect()
// 	return c
// }
//
// // collect waits for new requests and fulfills them.
// func (c *Collector) collect() {
// 	for {
// 		select {
// 		case req := <-c.reqs:
// 			for {
// 				bs, err := c.getBlocks(req.Remains())
// 				if err != nil {
// 					req.Fill(bs)
// 					req.Error(err)
// 					break
// 				}
//
// 				if !req.Fill(bs) {
// 					break
// 				}
// 			}
// 		case <-c.ctx.Done():
// 			return
// 		}
// 	}
// }
//
// // getBlocks reads up blocks by their ids but returns if max size limit is reached.
// func (c *Collector) getBlocks(ids []cid.Cid) (bs []blocks.Block, err error) {
// 	c.total = 0
// 	for _, id := range ids {
// 		size, err := c.blocks.GetSize(id)
// 		if err != nil {
// 			return bs, fmt.Errorf("can't get size of requested block(%s): %w", id, err)
// 		}
//
// 		if size > c.max {
// 			return bs, fmt.Errorf("found block bigger than limit message size")
// 		}
//
// 		c.total += size
// 		if c.total > c.max {
// 			return bs, nil
// 		}
//
// 		b, err := c.blocks.Get(id)
// 		if err != nil {
// 			return bs, fmt.Errorf("can't get requested block(%s): %w", id, err)
// 		}
//
// 		bs = append(bs, b)
// 	}
//
// 	return
// }
