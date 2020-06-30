package block

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"

	"github.com/Wondertan/go-blockstream/test"
)

func TestBlockCache(t *testing.T) {
	const limit = uint64(2048)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bs := blockstore.NewBlockstore(dsync.MutexWrap(datastore.NewMapDatastore()))
	bc := NewLimitedCache(ctx, bs, limit, false)

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

	bc.Close()
	bc.memory.Range(func(key, value interface{}) bool {
		t.Error("not empty")
		return false
	})
}
