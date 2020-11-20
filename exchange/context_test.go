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
