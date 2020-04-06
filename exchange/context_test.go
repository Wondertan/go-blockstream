package exchange

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContext_Providers(t *testing.T) {
	ctx := context.Background()

	peers, err := GetProviders(ctx)
	assert.Nil(t, peers)
	assert.Equal(t, ErrNoProviders, err)

	ctx = WithProviders(ctx, "test")
	peers, err = GetProviders(ctx)
	assert.Nil(t, err, err)
	assert.NotEmpty(t, peers)
}

func TestContext_Token(t *testing.T) {
	ctx := context.Background()

	tkn, err := GetToken(ctx)
	assert.Zero(t, tkn)
	assert.Equal(t, ErrNoToken, err)

	ctx = WithToken(ctx, "test")
	tkn, err = GetToken(ctx)
	assert.Nil(t, err, err)
	assert.NotZero(t, tkn)
}
