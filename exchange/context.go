package exchange

import (
	"context"
	"errors"

	"github.com/libp2p/go-libp2p-core/peer"
)

type (
	providersKey struct{}
)

var (
	ErrNoProviders = errors.New("blockstream: no providers")
	ErrNoToken     = errors.New("blockstream: no token")
)

func WithProviders(ctx context.Context, ids ...peer.ID) context.Context {
	if len(ids) == 0 {
		return ctx
	}

	return context.WithValue(ctx, providersKey{}, ids)
}

func GetProviders(ctx context.Context) ([]peer.ID, error) {
	ids, ok := ctx.Value(providersKey{}).([]peer.ID)
	if !ok {
		return nil, ErrNoProviders
	}

	return ids, nil
}
