package block

import (
	"container/list"
	"sync"
)

type RequestQueue struct {
	l    *list.List
	m    sync.RWMutex
	sig  chan struct{}
	done <-chan struct{}
}

func NewRequestQueue(done <-chan struct{}) *RequestQueue {
	return &RequestQueue{
		l:    list.New(),
		sig:  make(chan struct{}, 1),
		done: done,
	}
}

func (rq *RequestQueue) Len() int {
	rq.m.RLock()
	defer rq.m.RUnlock()
	return rq.l.Len()
}

func (rq *RequestQueue) Enqueue(reqs ...Request) {
	rq.m.Lock()
	defer rq.m.Unlock()

	for _, req := range reqs {
		rq.l.PushFront(req)
	}
	rq.signal()
}

func (rq *RequestQueue) Back() Request {
	select {
	case <-rq.sig:
		rq.m.RLock()
		e := rq.l.Back()
		rq.m.RUnlock()

		return e.Value.(Request)
	case <-rq.done:
		select {
		case <-rq.sig:
			rq.m.RLock()
			e := rq.l.Back()
			rq.m.RUnlock()

			return e.Value.(Request)
		default:
		}

		return nil
	}
}

func (rq *RequestQueue) BackPopDone() Request {
	for {
		rq.m.RLock()
		if rq.l.Back() != nil {
			rq.signal()
		}
		rq.m.RUnlock()

		req := rq.Back()
		if req == nil {
			return nil
		}

		select {
		case <-req.Done():
			rq.m.Lock()
			rq.l.Remove(rq.l.Back())
			rq.m.Unlock()
			continue
		default:
			return req
		}
	}
}

func (rq *RequestQueue) PopBack() {
	rq.m.Lock()
	rq.l.Remove(rq.l.Back())
	if rq.l.Back() != nil {
		rq.signal()
	}
	rq.m.Unlock()
}

func (rq *RequestQueue) ForEach(f func(req Request)) {
	rq.m.RLock()
	defer rq.m.RUnlock()

	for e := rq.l.Front(); e != nil; e = e.Next() {
		f(e.Value.(Request))
	}
}

func (rq *RequestQueue) Cancel(id RequestID) {
	req := rq.get(id)
	if req != nil {
		req.Cancel()
	}
}

func (rq *RequestQueue) get(id RequestID) Request {
	rq.m.RLock()
	defer rq.m.RUnlock()

	for e := rq.l.Front(); e != nil; e = e.Next() {
		if e.Value.(Request).ID().Equals(id) {
			return e.Value.(Request)
		}
	}

	return nil
}

func (rq *RequestQueue) signal() {
	select {
	case rq.sig <- struct{}{}:
	default:
	}
}
