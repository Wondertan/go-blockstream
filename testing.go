package blockstream

import (
	"fmt"
	"io"
	"testing"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs-blockstore"
	"github.com/stretchr/testify/assert"
)

func randBlockstore(t *testing.T, rand io.Reader, count, size int) (blockstore.Blockstore, []cid.Cid) {
	bstore := blockstore.NewBlockstore(sync.MutexWrap(datastore.NewMapDatastore()))
	bs, ids := randBlocks(t, rand, count, size)
	for _, b := range bs {
		err := bstore.Put(b)
		if err != nil {
			t.Fatal(err)
		}
	}

	return bstore, ids
}

func randBlocks(t *testing.T, rand io.Reader, count, size int) ([]blocks.Block, []cid.Cid) {
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

func assertChan(t *testing.T, ch <-chan blocks.Block, bs blockstore.Blockstore, expected int) {
	var actual int
	for b := range ch {
		ok, err := bs.Has(b.Cid())
		assert.Nil(t, err, err)
		assert.True(t, ok)
		actual++
		fmt.Println(actual)
	}
	assert.Equal(t, expected, actual)
}

type fakeStream struct {
	read  *io.PipeReader
	write *io.PipeWriter
}

func pair() (*fakeStream, *fakeStream) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	return &fakeStream{r1, w2}, &fakeStream{r2, w1}
}

func (s *fakeStream) Read(b []byte) (n int, err error) {
	return s.read.Read(b)
}

func (s *fakeStream) Write(b []byte) (n int, err error) {
	return s.write.Write(b)
}

func (s *fakeStream) Close() error {
	return s.write.Close()
}
