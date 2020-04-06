package blockstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGranterSuccess(t *testing.T) {
	const (
		tkn    = Token("test")
		p1, p2 = peer.ID("peer1"), peer.ID("peer2")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	granter := NewAccessGranter()

	ch := granter.Grant(ctx, tkn, p1, p2)
	require.NotNil(t, ch)

	g1, err := granter.Granted(tkn, p1)
	require.Nil(t, err)

	go func() { g1 <- nil }()
	select {
	case <-ch:
		t.Fatal("receive value before everything done")
	default:
	}

	g2, err := granter.Granted(tkn, p2)
	require.Nil(t, err)

	go func() { g2 <- nil }()
	err, ok := <-ch
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestGranterError(t *testing.T) {
	const (
		tkn    = Token("test")
		p1, p2 = peer.ID("peer1"), peer.ID("peer2")
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	granter := NewAccessGranter()
	in := fmt.Errorf("test")

	ch := granter.Grant(ctx, tkn, p1, p2)
	require.NotNil(t, ch)

	g1, err := granter.Granted(tkn, p1)
	require.Nil(t, err)

	go func() { g1 <- in }()
	out := <-ch
	assert.Equal(t, NewError(p1, tkn, in), out)

	g2, err := granter.Granted(tkn, p2)
	require.Nil(t, err)

	go func() { g2 <- in }()
	out = <-ch
	assert.Equal(t, NewError(p2, tkn, in), out)

	out, ok := <-ch
	assert.Nil(t, out)
	assert.False(t, ok)
}

func TestGranterFail(t *testing.T) {
	const p = peer.ID("test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	granter := NewAccessGranter()

	ch1 := granter.Grant(ctx, "test1", p)
	assert.NotNil(t, ch1)

	tkn := Token("test2")
	ch2, err := granter.Granted(tkn, p)
	assert.Equal(t, NewError(p, tkn, ErrNotGranted), err)
	assert.Nil(t, ch2)
}
