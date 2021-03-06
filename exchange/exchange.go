package exchange

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	log2 "github.com/ipfs/go-log"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	iexchange "github.com/ipfs/go-ipfs-exchange-interface"

	"github.com/Wondertan/go-blockstream"
)

var log = log2.Logger("stream-exchange")

type exchange blockstream.BlockStream

func New(bs *blockstream.BlockStream) iexchange.SessionExchange {
	return (*exchange)(bs)
}

func (e *exchange) GetBlock(ctx context.Context, id cid.Cid) (blocks.Block, error) {
	return getBlock(ctx, id, e.GetBlocks)
}

func (e *exchange) GetBlocks(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, error) {
	ses := e.NewSession(ctx)
	return ses.GetBlocks(ctx, ids)
}

func (e *exchange) HasBlock(blocks.Block) error {
	return nil
}

func (e *exchange) IsOnline() bool {
	return true
}

func (e *exchange) Close() error {
	return (*blockstream.BlockStream)(e).Close()
}

type fetcher struct {
	ses *blockstream.Session
	err error
}

func (e *exchange) NewSession(ctx context.Context) iexchange.Fetcher {
	prvs, err := GetProviders(ctx)
	if err != nil {
		return &fetcher{err: err}
	}

	ses, err := (*blockstream.BlockStream)(e).Session(ctx, prvs)
	return &fetcher{ses: ses, err: err}
}

func (f *fetcher) GetBlock(ctx context.Context, id cid.Cid) (blocks.Block, error) {
	return getBlock(ctx, id, f.GetBlocks)
}

func (f *fetcher) GetBlocks(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, error) {
	if f.err != nil {
		return nil, f.err
	}

	outB := make(chan blocks.Block)
	go func() {
		defer close(outB)
		resCh, errCh := f.ses.Blocks(ctx, ids)
		for res := range resCh {
			if res.Block == nil {
				log.Warnf("Failed to retrieve %s: %s", res.Cid, res.Err)
				continue
			}

			select {
			case outB <- res.Block:
			case <-ctx.Done():
				return
			}
		}

		err := <-errCh
		if err != nil {
			log.Errorf("Stream failed with: %s", err)
		}
	}()

	return outB, nil
}

func getBlock(
	ctx context.Context,
	id cid.Cid,
	blocks func(ctx context.Context, ids []cid.Cid,
	) (<-chan blocks.Block, error)) (blocks.Block, error) {
	ch, err := blocks(ctx, []cid.Cid{id})
	if err != nil {
		return nil, err
	}

	block, ok := <-ch
	if !ok {
		return nil, blockstore.ErrNotFound
	}

	return block, nil
}
