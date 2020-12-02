package blocknet

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/Wondertan/go-blockstream/test"
)

func TestRequester(t *testing.T) {
	const (
		count = 32
		size  = 32
	)

	r, m := newTestRequester()
	bs, ids := test.RandBlocks(t, rand.Reader, count, size)

	t.Run("Request", func(t *testing.T) {
		req := r.NewRequest(ids[0], ids)
		require.Equal(t, req.ID(), ids[0])
		require.Equal(t, req.Ids(), ids)

		t.Run("Have", func(t *testing.T) {
			m.OnMessage(HAVE, func(msg Message) {
				id, err := HaveRequest(msg)
				require.NoError(t, err)
				require.Equal(t, req.ID(), id)
			})
			req.Have()
		})

		t.Run("Cancel", func(t *testing.T) {
			m.OnMessage(CANCEL, func(msg Message) {
				id, err := CancelRequest(msg)
				require.NoError(t, err)
				require.Equal(t, req.ID(), id)
			})
			req.Cancel()
		})

		t.Run("Next", func(t *testing.T) {
			t.Run("Success", func(t *testing.T) {
				req = r.NewRequest(ids[0], ids)

				m.OnMessage(REQUEST, func(msg Message) {
					id, _, err := BlocksRequest(msg)
					require.NoError(t, err)

					err = m.Send(BlocksResponseMsg(id, bs))
					require.NoError(t, err)
				})

				for i := 0; i < count; i++ {
					o, ok := req.Next()
					require.True(t, ok)

					b, err := o.Get()
					require.NoError(t, err)
					require.Equal(t, bs[i], b)
				}
			})

			t.Run("End", func(t *testing.T) {
				o, ok := req.Next()
				require.False(t, ok)
				require.Nil(t, o)
			})

			t.Run("Error", func(t *testing.T) {
				req = r.NewRequest(ids[0], ids)

				m.OnMessage(REQUEST, func(msg Message) {
					id, _, err := BlocksRequest(msg)
					require.NoError(t, err)

					err = m.Send(ErrorResponseMsg(id, unknownError))
					require.NoError(t, err)
				})

				for i := 0; i < count; i++ {
					o, ok := req.Next()
					require.True(t, ok)
					require.Equal(t, bs[i].Cid(), o.Cid())

					b, err := o.Get()
					require.Error(t, err)
					require.Nil(t, b)
				}
			})
		})
	})

	t.Run("Close", func(t *testing.T) {
		err := r.Close()
		require.NoError(t, err)
		<-m.Done()
	})
}
