package streaming

import (
	"io"
	"testing"

	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs-blockstore"
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
