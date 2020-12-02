package blocknet

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/Wondertan/go-serde"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream/blocknet/pb"
)

type (
	MessageHandler func(Message)
	CloseHandler   func(func() error)
)

type Messenger struct {
	peer peer.ID

	sl  sync.Mutex
	rwc io.ReadWriteCloser

	ml sync.Mutex
	hs map[MessageType]MessageHandler

	done chan struct{}
}

func NewMessenger(rwc io.ReadWriteCloser, peer peer.ID, ch CloseHandler) *Messenger {
	r := &Messenger{
		rwc:  rwc,
		peer: peer,
		hs:   make(map[MessageType]MessageHandler),
		done: make(chan struct{}),
	}

	go ch(r.handle)
	return r
}

func (m *Messenger) Peer() peer.ID {
	return m.peer
}

func (m *Messenger) Send(msg Message) error {
	m.sl.Lock()
	defer m.sl.Unlock()

	_, err := serde.Write(m.rwc, msg)
	if err != nil {
		return fmt.Errorf("can't write message: %w", err)
	}

	return nil
}

func (m *Messenger) OnMessage(tp MessageType, h MessageHandler) {
	m.ml.Lock()
	defer m.ml.Unlock()

	m.hs[tp] = h
}

func (m *Messenger) Close() error {
	m.sl.Lock()
	defer m.sl.Unlock()

	return m.rwc.Close()
}

func (m *Messenger) Done() <-chan struct{} {
	return m.done
}

func (m *Messenger) handle() error {
	defer close(m.done)

	for {
		msg := new(pb.BlockStream)
		_, err := serde.Read(m.rwc, msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("can't read message: %w", err)
		}

		m.ml.Lock()
		h, ok := m.hs[msg.Type]
		m.ml.Unlock()

		if ok {
			h(msg)
		}
	}
}
