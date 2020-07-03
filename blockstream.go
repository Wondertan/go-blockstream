package blockstream

import (
	"context"
	"sync"

	"github.com/Wondertan/go-libp2p-access"
	"github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/Wondertan/go-blockstream/block"
)

var log = logging.Logger("blockstream")

const Protocol protocol.ID = "/blockstream/1.0.0"

const collectorsDefault = 8

type BlockStream struct {
	ctx context.Context

	Host    host.Host
	Granter access.Granter
	Blocks  blockstore.Blockstore

	reqs chan *block.Request

	collectors int

	wg sync.WaitGroup
}

type Option func(plain *BlockStream)

func Collectors(c int) Option {
	return func(bs *BlockStream) {
		bs.collectors = c
	}
}

func NewBlockStream(ctx context.Context, host host.Host, bstore blockstore.Blockstore, granter access.Granter, opts ...Option) *BlockStream {
	bs := &BlockStream{
		ctx:        ctx,
		Host:       host,
		Granter:    granter,
		Blocks:     bstore,
		reqs:       make(chan *block.Request, 16),
		collectors: collectorsDefault,
	}
	for _, opt := range opts {
		opt(bs)
	}

	for range make([]bool, collectorsDefault) {
		block.NewCollector(ctx, bs.reqs, bstore, maxMsgSize)
	}

	host.SetStreamHandler(Protocol, func(s network.Stream) {
		err := bs.handler(s)
		if err != nil {
			log.Error(err)
			s.Reset()
		}
	})
	return bs
}

func (bs *BlockStream) Close() error {
	bs.wg.Wait()
	return nil
}

// TODO Define opts.
// Session starts new BlockStream session between current node and providing 'peers' within the `token` namespace.
// Autosave defines if received Blocks should be automatically put into Blockstore.
func (bs *BlockStream) Session(ctx context.Context, token access.Token, autosave bool, peers ...peer.ID) (*Session, error) {
	ses := newSession(ctx)
	for _, p := range peers {
		s, err := bs.Host.NewStream(ctx, p, Protocol)
		if err != nil {
			return nil, err
		}

		err = giveHand(s, token)
		if err != nil {
			s.Reset()
			return nil, err
		}

		ses.addProvider(s, func(f func() error) {
			bs.wg.Add(1)
			defer bs.wg.Done()

			if err := f(); err != nil {
				log.Error(err)
				s.Reset()
			}
		})
	}

	return ses, nil
}

func (bs *BlockStream) handler(s network.Stream) error {
	var done chan<- error
	_, err := takeHand(s, func(t access.Token) (err error) {
		done, err = bs.Granter.Granted(t, s.Conn().RemotePeer())
		return
	})
	if err != nil {
		return err
	}

	var once sync.Once
	newResponder(bs.ctx, s, bs.reqs,
		func(f func() error) {
			bs.wg.Add(1)
			defer bs.wg.Done()

			err := f()
			if err != nil {
				log.Error(err)
			}

			once.Do(func() {
				if err != nil {
					s.Reset()
					done <- err
					return
				}

				close(done)
			})
		},
	)
	return nil
}

type onToken func(access.Token) error
type сlose func(func() error)

var logClose = func(f func() error) {
	err := f()
	if err != nil {
		log.Error(err)
	}
}
