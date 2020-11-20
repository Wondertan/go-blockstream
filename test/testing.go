package test

import (
	"io"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

func EmptyBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(dsync.MutexWrap(datastore.NewMapDatastore()))
}

func RandBlockstore(t *testing.T, rand io.Reader, count, size int) (blockstore.Blockstore, []cid.Cid) {
	bstore := EmptyBlockstore()
	bs, ids := RandBlocks(t, rand, count, size)
	for _, b := range bs {
		err := bstore.Put(b)
		if err != nil {
			t.Fatal(err)
		}
	}

	return bstore, ids
}

func RandBlocks(t *testing.T, rand io.Reader, count, size int) ([]blocks.Block, []cid.Cid) {
	bs := make([]blocks.Block, count)
	ids := make([]cid.Cid, count)
	for i := 0; i < count; i++ {
		b := make([]byte, size)
		_, err := rand.Read(b)
		if err != nil {
			t.Fatal(err)
		}

		bs[i] = blocks.NewBlock(b)
		ids[i] = bs[i].Cid()
	}

	return bs, ids
}
