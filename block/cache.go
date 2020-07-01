package block

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

const savers = 8

var prefix = datastore.NewKey("blockstream")

// Cache is cache used within streams
type Cache interface {
	Add(blocks.Block)
	Get(cid.Cid) blocks.Block
	Has(cid.Cid) bool
}

type limitedCache struct {
	memory sync.Map
	disk   datastore.Datastore

	memoryUsage, memoryLimit uint64

	ctx     context.Context
	toSave  chan blocks.Block
	memLock chan struct{}
}

func NewLimitedCache(ctx context.Context, store datastore.Datastore, memoryLimit uint64) *limitedCache {
	bc := &limitedCache{
		disk:        namespace.Wrap(store, prefix),
		memoryLimit: memoryLimit,
		ctx:         ctx,
		toSave:      make(chan blocks.Block, 32),
		memLock:     make(chan struct{}, 1),
	}

	for i := 0; i < savers; i++ {
		go bc.saver()
	}

	go func() {
		<-ctx.Done()
		bc.memory.Range(func(key, value interface{}) bool {
			err := bc.disk.Delete(dshelp.MultihashToDsKey(key.(cid.Cid).Hash()))
			if err != nil {
				log.Errorf("Can't delete cached item: %w", err)
			}

			bc.memory.Delete(key)
			return true
		})
	}()

	return bc
}

func (bc *limitedCache) MemoryUsage() uint64 {
	return atomic.LoadUint64(&bc.memoryUsage)
}

func (bc *limitedCache) Add(b blocks.Block) {
	bc.memory.Store(b.Cid(), b)
	atomic.AddUint64(&bc.memoryUsage, uint64(len(b.RawData())))

	select {
	case bc.toSave <- b:
	case <-bc.ctx.Done():
		return
	}
}

func (bc *limitedCache) Get(id cid.Cid) blocks.Block {
	e, _ := bc.memory.Load(id) // try getting entry from the Map
	if e == nil {
		bdata, err := bc.disk.Get(dshelp.MultihashToDsKey(id.Hash())) // if not in map, should be on a disk.
		if err != nil && !errors.Is(err, datastore.ErrNotFound) {
			log.Errorf("Can't get the Block(%s): %w", id.String(), err)
		}

		b, _ := blocks.NewBlockWithCid(bdata, id)
		return b // or requested before it was added.
	}
	b := e.(blocks.Block)

	bc.memory.Store(id, nil)                                       // remove pointer on block for GC to clean it later
	atomic.AddUint64(&bc.memoryUsage, ^uint64(len(b.RawData())-1)) // reduce in use memory
	return b
}

func (bc *limitedCache) Has(id cid.Cid) bool {
	_, ok := bc.memory.Load(id)
	return ok
}

func (bc *limitedCache) saver() {
	for {
		select {
		case b := <-bc.toSave:
			err := bc.disk.Put(dshelp.MultihashToDsKey(b.Cid().Hash()), b.RawData())
			if err != nil {
				log.Errorf("Can't save the Block(%s): %w", b.Cid().String(), err)
				continue
			}

			bc.freeMemory()
		case <-bc.ctx.Done():
			return
		}
	}
}

func (bc *limitedCache) freeMemory() {
	if bc.MemoryUsage() > bc.memoryLimit {
		select {
		case bc.memLock <- struct{}{}:
			bc.memory.Range(func(key, value interface{}) bool {
				need := int(bc.MemoryUsage() - bc.memoryLimit)
				l := len(value.(blocks.Block).RawData())
				bc.memory.Delete(key)
				atomic.AddUint64(&bc.memoryUsage, ^uint64(l-1))
				return l < need
			})
			<-bc.memLock
		default:
		}
	}
}

type simpleCache struct {
	m sync.Map
}

func NewSimpleCache() *simpleCache {
	return &simpleCache{}
}

func (s *simpleCache) Add(block blocks.Block) {
	s.m.Store(block.Cid(), block)
}

func (s *simpleCache) Get(c cid.Cid) blocks.Block {
	b, ok := s.m.Load(c)
	if !ok {
		return nil
	}

	s.m.Delete(c)
	return b.(blocks.Block)
}

func (s *simpleCache) Has(c cid.Cid) bool {
	_, ok := s.m.Load(c)
	return ok
}
