package block

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

const savers = 8

// Cache is cache used within streams
type Cache interface {
	Add(blocks.Block)
	Get(cid.Cid) blocks.Block
	Has(cid.Cid) bool
}

type limitedCache struct {
	memory sync.Map
	disk   blockstore.Blockstore

	memoryUsage, memoryLimit uint64

	ctx     context.Context
	toSave  chan blocks.Block
	memLock chan struct{}
}

func NewLimitedCache(ctx context.Context, store blockstore.Blockstore, memoryLimit uint64, autosave bool) *limitedCache {
	bc := &limitedCache{
		disk:        store,
		memoryLimit: memoryLimit,
		ctx:         ctx,
		toSave:      make(chan blocks.Block, 32),
		memLock:     make(chan struct{}, 1),
	}

	for i := 0; i < savers; i++ {
		go bc.saver()
	}

	if !autosave {
		go func() {
			<-ctx.Done()
			bc.memory.Range(func(key, value interface{}) bool {
				err := bc.disk.DeleteBlock(key.(cid.Cid))
				if err != nil {
					log.Errorf("Can't delete Block on closing: %w", err)
				}

				bc.memory.Delete(key)
				return true
			})
		}()
	}

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
	if !id.Defined() {
		fmt.Println("hi")
	}
	e, _ := bc.memory.Load(id) // try getting entry from the Map
	if e == nil {
		b, err := bc.disk.Get(id) // if not in map, should be on a disk.
		if err != nil && !errors.Is(err, blockstore.ErrNotFound) {
			log.Errorf("Can't get the block: %w", err)
		}

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
			err := bc.disk.Put(b)
			if err != nil {
				log.Errorf("Can't save the block: %w", err)
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
	return b.(blocks.Block)
}

func (s *simpleCache) Has(c cid.Cid) bool {
	_, ok := s.m.Load(c)
	return ok
}
