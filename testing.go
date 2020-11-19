package blockstream

import (
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/block"
)

func assertBlockReq(t *testing.T, r io.Reader, in uint32, ids []cid.Cid) {
	id, out, err := readBlocksReq(r)
	require.Nil(t, err, err)
	assert.Equal(t, ids, out)
	assert.Equal(t, in, id)
}

func assertBlockResp(t *testing.T, r io.Reader, in uint32, ids []cid.Cid, errIn error) {
	id, out, errOut, err := readBlocksResp(r)
	require.Nil(t, err, err)
	assert.Equal(t, in, id)
	assert.Equal(t, len(ids), len(out))
	assert.Equal(t, errIn, errOut)
	for i, b := range out {
		_, err = newBlockCheckCid(b, ids[i])
		require.Nil(t, err, err)
	}
}

func newRequestPair(ctx context.Context, in, out chan *block.Request) {
	s1, s2 := streamPair()
	newRequester(ctx, s1, in, logClose)
	newResponder(ctx, s2, out, logClose)
}

func newTestResponder(t *testing.T, ctx context.Context, reqs chan *block.Request) io.ReadWriter {
	s1, s2 := streamPair()
	newResponder(ctx, s2, reqs, logClose)
	return s1
}

func newTestRequester(t *testing.T, ctx context.Context, reqs chan *block.Request) io.ReadWriter {
	s1, s2 := streamPair()
	newRequester(ctx, s2, reqs, logClose)
	return s1
}

type fakeStream struct {
	read  *io.PipeReader
	write *io.PipeWriter
}

func streamPair() (*fakeStream, *fakeStream) {
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
