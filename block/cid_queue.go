package block

import (
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
)

// TODO This better be lock-free
// cidQueue is a linked list of CIDs that implements queue.
type cidQueue struct {
	len         uint32
	back, front *cidQueueItem
	l           sync.Mutex
}

// cidQueueItem is an element of cidQueue.
type cidQueueItem struct {
	cid  cid.Cid
	next *cidQueueItem
}

// newCidQueue creates new cidQueue.
func newCidQueue() *cidQueue {
	itm := &cidQueueItem{}
	return &cidQueue{back: itm, front: itm}
}

// Len returns length of the queue.
func (l *cidQueue) Len() uint32 {
	return atomic.LoadUint32(&l.len)
}

// Enqueue adds given CIDs to the front of the queue.
// Must be called only from one goroutine.
func (l *cidQueue) Enqueue(ids ...cid.Cid) {
	l.l.Lock()
	defer l.l.Unlock()

	ln := uint32(len(ids))
	if !l.front.cid.Defined() {
		l.front.cid = ids[0]
		ids = ids[1:]
	}

	for _, id := range ids {
		if !id.Defined() {
			ln--
			continue
		}

		l.front.next = &cidQueueItem{cid: id}
		l.front = l.front.next
	}

	atomic.AddUint32(&l.len, ln)
}

// Dequeue removes and returns last CID from the queue.
// Must be called only from one goroutine.
func (l *cidQueue) Dequeue() cid.Cid {
	l.l.Lock()
	defer l.l.Unlock()

	id := l.back.cid
	if id.Defined() {
		if l.back.next != nil {
			l.back = l.back.next
		} else {
			l.back.cid = cid.Undef
		}

		atomic.AddUint32(&l.len, ^uint32(0))
	}

	return id
}
