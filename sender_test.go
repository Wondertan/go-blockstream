package streaming

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSender(t *testing.T) {
	const (
		reqsCount   = 3
		blocksCount = 32
		blockSize   = 32
		msgSize     = 128
		perMsg      = msgSize / blockSize
	)

	in := Token("test")
	bs, ids := randBlockstore(t, rand.Reader, blocksCount*reqsCount, blockSize)
	s1, s2 := pair()

	go giveHand(s2, in)
	_, err := newSender(s1, bs, msgSize,
		func(out Token) error {
			assert.Equal(t, in, out)
			return nil
		},
		func(f func() error) {
			if err := f(); err != nil {
				t.Error(err)
			}
		},
	)
	require.Nil(t, err, err)

	start := 0
	for range make([]bool, reqsCount) {
		err := writeBlocksReq(s2, ids[start:start+blocksCount])
		require.Nil(t, err, err)

		for range make([]bool, blocksCount/perMsg) {
			bs, err := readBlocksResp(s2, ids[start:start+perMsg])
			require.Nil(t, err, err)
			assert.Len(t, bs, perMsg)

			start += perMsg
		}
	}
}
