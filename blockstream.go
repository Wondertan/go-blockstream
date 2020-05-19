package blockstream

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-datastore"
	dsync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("blockstream")

const Protocol protocol.ID = "/blockstream/1.0.0"

var errClosed = errors.New("blockstream: closed")

type BlockStream struct {
	Host    host.Host
	Granter access.Granter
	Blocks  blockstore.Blockstore

	sessions struct {
		m  map[access.Token]*Session
		l  sync.Mutex
		wg sync.WaitGroup
	}

	senders struct {
		m  map[access.Token]*sender
		l  sync.Mutex
		wg sync.WaitGroup
	}

	closed chan struct{}
}

type Option func(plain *BlockStream)

func NewBlockStream(host host.Host, bstore blockstore.Blockstore, granter access.Granter, opts ...Option) *BlockStream {
	bs := &BlockStream{
		Host:    host,
		Granter: granter,
		Blocks:  bstore,
		sessions: struct {
			m  map[access.Token]*Session
			l  sync.Mutex
			wg sync.WaitGroup
		}{m: make(map[access.Token]*Session)},
		senders: struct {
			m  map[access.Token]*sender
			l  sync.Mutex
			wg sync.WaitGroup
		}{m: make(map[access.Token]*sender)},
		closed: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(bs)
	}

	host.SetStreamHandler(Protocol, func(stream network.Stream) {
		err := bs.handler(stream)
		if err != nil {
			log.Error(err)
		}
	})
	return bs
}

func (bs *BlockStream) Close() error {
	if bs.isClosed() {
		return errClosed
	}

	close(bs.closed)
	bs.sessions.wg.Wait()
	bs.senders.wg.Wait()
	return nil
}

// TODO Define opts.
// Session starts new BlockStream session between current node and providing 'peers' within the `token` namespace.
// Autosave defines if received Blocks should be automatically put into Blockstore.
func (bs *BlockStream) Session(ctx context.Context, token access.Token, autosave bool, peers ...peer.ID) (ses *Session, err error) {
	if bs.isClosed() {
		return nil, errClosed
	}

	streams := make([]io.ReadWriteCloser, len(peers))
	for i, p := range peers {
		streams[i], err = bs.Host.NewStream(ctx, p, Protocol)
		if err != nil {
			return
		}
	}

	var store blockstore.Blockstore
	if autosave {
		store = bs.Blocks
	} else {
		store = newBlockstore()
	}

	ses, err = newSession(
		ctx,
		store,
		streams,
		token,
		func(f func() error) {
			if err := f(); err != nil {
				log.Error(err)
			}
		},
	)
	if err != nil {
		return
	}

	bs.sessions.l.Lock()
	bs.sessions.m[token] = ses
	bs.sessions.wg.Add(1)
	bs.sessions.l.Unlock()

	go func() {
		<-ctx.Done()
		bs.sessions.l.Lock()
		delete(bs.sessions.m, token)
		bs.sessions.wg.Done()
		bs.sessions.l.Unlock()
	}()

	return
}

func (bs *BlockStream) handler(stream network.Stream) error {
	var (
		done chan<- error
		tkn  access.Token
	)
	s, err := newSender(stream, bs.Blocks, maxMsgSize,
		func(t access.Token) (err error) {
			done, err = bs.Granter.Granted(tkn, stream.Conn().RemotePeer())
			tkn = t
			return
		},
		func(f func() error) {
			if err := f(); err != nil {
				log.Error(err)
				done <- err
			}

			bs.senders.l.Lock()
			delete(bs.senders.m, tkn)
			bs.senders.wg.Done()
			bs.senders.l.Unlock()
		},
	)
	if err != nil {
		return err
	}

	bs.senders.l.Lock()
	bs.senders.m[s.t] = s
	bs.senders.wg.Add(1)
	bs.senders.l.Unlock()

	return nil
}

func (bs *BlockStream) isClosed() bool {
	select {
	case <-bs.closed:
		return true
	default:
		return false
	}
}

type onToken func(access.Token) error
type onClose func(func() error)

func newBlockstore() blockstore.Blockstore {
	return blockstore.NewBlockstore(dsync.MutexWrap(datastore.NewMapDatastore()))
}
