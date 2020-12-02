package blockstream

import (
	"context"
	"errors"
	"sync"

	access "github.com/Wondertan/go-libp2p-access"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"

	"github.com/Wondertan/go-blockstream/block"
)

var ErrNoProviders = errors.New("blockstream: no providers")

var log = logging.Logger("blockstream")

const Protocol protocol.ID = "/blockstream/1.0.0"

const collectorsDefault = 32

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

// Session starts new BlockStream session between current node and providing 'peers'.
func (bs *BlockStream) Session(ctx context.Context, peers []peer.ID, opts ...SessionOption) (*Session, error) {
	ses := newSession(ctx, opts...)
	if ses.offline {
		return ses, nil
	}

	tkn, err := access.GetToken(ctx)
	if err != nil {
		return nil, err
	}

	for _, p := range peers {
		s, err := bs.Host.NewStream(ctx, p, Protocol)
		if err != nil {
			return nil, err
		}

		err = giveHand(s, tkn)
		if err != nil {
			s.Reset()
			return nil, err
		}

		var once sync.Once
		ses.addProvider(s, func(f func() error) {
			bs.wg.Add(1)
			defer bs.wg.Done()

			if err := f(); err != nil {
				once.Do(func() {
					s.Reset()
					log.Errorf("Failed provider %s for session %s: %s", p.Pretty(), tkn, err)

					if ses.removeProvider() == 0 {
						log.Errorf("Terminating session %s: %s", tkn, err)

						ses.err = ErrNoProviders
						ses.cancel()
					}
				})
			}
		})
	}

	return ses, nil
}

func (bs *BlockStream) handler(s network.Stream) error {
	p := s.Conn().RemotePeer()
	var done chan<- error
	_, err := takeHand(s, func(t access.Token) (err error) {
		done, err = bs.Granter.Granted(t, p)
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
				log.Errorf("Failed to respond to peer %s: %s", p.Pretty(), err)
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

type (
	onToken func(access.Token) error
	сlose   func(func() error)
)

var logClose = func(f func() error) {
	err := f()
	if err != nil {
		log.Error(err)
	}
}
