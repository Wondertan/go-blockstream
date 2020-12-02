package blocknet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessenger(t *testing.T) {
	m1, m2 := NewTestMessengerPair()
	msg := newFakeMessage(t)

	t.Run("SendAndOnMessage", func(t *testing.T) {
		mch := make(chan Message)
		m2.OnMessage(msg.GetType(), func(msg Message) {
			mch <- msg
		})

		err := m1.Send(msg)
		require.NoError(t, err)
		assert.Equal(t, msg, <-mch)
	})

	t.Run("CloseAndDone", func(t *testing.T) {
		err := m2.Close()
		require.NoError(t, err)
		<-m1.Done()

		err = m2.Send(msg)
		assert.Error(t, err)
	})

	t.Run("Peer", func(t *testing.T) {
		assert.Equal(t, peer1, m1.Peer())
		assert.Equal(t, peer2, m2.Peer())
	})
}
