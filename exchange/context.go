package exchange

import (
	"context"
	"errors"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream"
)

type (
	providersKey struct{}
	tokenKey     struct{}
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

func WithToken(ctx context.Context, token blockstream.Token) context.Context {
	if len(token) == 0 {
		return ctx
	}

	return context.WithValue(ctx, tokenKey{}, token)
}

func GetToken(ctx context.Context) (blockstream.Token, error) {
	token, ok := ctx.Value(tokenKey{}).(blockstream.Token)
	if !ok {
		return "", ErrNoToken
	}

	return token, nil
}
