package block

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"

	"github.com/Wondertan/go-blockstream/test"
)

func TestLimitedCache(t *testing.T) {
	const limit = uint64(2048)

	ctx, cancel := context.WithCancel(context.Background())

	ds := dsync.MutexWrap(datastore.NewMapDatastore())
	bc := NewLimitedCache(ctx, ds, limit)

	bls, ids := test.RandBlocks(t, rand.Reader, 32, 128)
	for _, b := range bls {
		bc.Add(b)
	}
	assert.LessOrEqual(t, limit, bc.MemoryUsage())

	time.Sleep(time.Millisecond * 10)
	assert.Equal(t, limit, bc.MemoryUsage())

	for _, id := range ids {
		b := bc.Get(id)
		assert.NotNil(t, b)
	}
	assert.Equal(t, uint64(0), bc.MemoryUsage())

	cancel()
	time.Sleep(time.Millisecond)

	bc.memory.Range(func(key, value interface{}) bool {
		t.Error("not empty")
		return false
	})
}

func TestSimpleCache(t *testing.T) {
	c := NewSimpleCache()

	bs, ids := test.RandBlocks(t, rand.Reader, 32, 128)
	for _, b := range bs {
		c.Add(b)
	}

	for i, id := range ids {
		assert.True(t, c.Has(id))
		b := c.Get(id)
		assert.Equal(t, bs[i], b)
	}

	c.m.Range(func(key, value interface{}) bool {
		t.Error("not empty")
		return true
	})
}
