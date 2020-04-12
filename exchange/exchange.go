package exchange

import (
	"context"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-ipfs-blockstore"
	iexchange "github.com/ipfs/go-ipfs-exchange-interface"

	"github.com/Wondertan/go-blockstream"
)

type exchange blockstream.BlockStream

func New(bs *blockstream.BlockStream) iexchange.Interface {
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

	tkn, err := GetToken(ctx)
	if err != nil {
		return &fetcher{err: err}
	}

	ses, err := (*blockstream.BlockStream)(e).Session(ctx, tkn, prvs...)
	go func() {
		<-ctx.Done()
		ses.Close()
	}()
	return &fetcher{ses: ses, err: err}
}

func (f *fetcher) GetBlock(ctx context.Context, id cid.Cid) (blocks.Block, error) {
	return getBlock(ctx, id, f.GetBlocks)
}

func (f *fetcher) GetBlocks(ctx context.Context, ids []cid.Cid) (<-chan blocks.Block, error) {
	if f.err != nil {
		return nil, f.err
	}

	return f.ses.Blocks(ctx, ids), nil
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
