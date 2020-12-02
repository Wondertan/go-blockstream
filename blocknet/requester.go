package blocknet

import (
	"context"
	"errors"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"github.com/Wondertan/go-blockstream/block"
)

type Requester struct {
	ms *Messenger
	rq *block.RequestQueue

	reqs map[block.RequestID]block.Request
	l sync.Mutex
}

func NewRequester(ms *Messenger) *Requester {
	r := &Requester{ms: ms, rq: block.NewRequestQueue(nil), reqs: make(map[block.RequestID]block.Request)}
	ms.OnMessage(RESPONSE, r.onResponse)
	ms.OnMessage(ERROR, r.onErrorResponse)
	return r
}

func (r *Requester) NewRequest(id block.RequestID, ids []cid.Cid) block.Request {
	r.l.Lock()
	defer r.l.Unlock()

	req, ok := r.reqs[id]
	if !ok {
		req = &request{r: r, id: id, ids: ids, done: make(chan struct{})}
		r.reqs[id] = req
		return req
	}

	return req
}

func (r *Requester) Send(req block.Request) {
	err := r.ms.Send(BlocksRequestMsg(req.ID(), req.Ids()))
	if err != nil {
		log.Errorf("can't send request(%s): %s", req.ID(), err)
		return
	}

	r.rq.Enqueue(req)
}

func (r *Requester) Cancel(id block.RequestID) {
	err := r.ms.Send(CancelRequestMsg(id))
	if err != nil {
		log.Errorf("can't cancel request(%s): %s", id, err)
		return
	}
}

func (r *Requester) Have(id block.RequestID) {
	err := r.ms.Send(HaveRequestMsg(id))
	if err != nil {
		log.Errorf("can't send Have message(%s): %s", id, err)
		return
	}
}

func (r *Requester) Close() error {
	return r.ms.Close()
}

func (r *Requester) Shut(ctx context.Context) error {
	return nil
}

func (r *Requester) onResponse(msg Message) {
	id, data, err := BlocksResponse(msg)
	if err != nil {
		return
	}

	req := r.rq.BackPopDone()
	if req == nil {
		return
	}

	if req.ID() != id {
		log.Warnf("Received Block response for wrong request(%d), skipping...", id)
		return
	}

	ids := req.Ids()
	bs := make([]blocks.Block, len(data))
	for i, b := range data {
		bs[i], err = newBlockCheckCid(b, ids[i])
		if err != nil {
			if errors.Is(err, blocks.ErrWrongHash) {
				log.Errorf("%s: expected: %s, received: %s", err, ids[i], bs[i])
			}

			return
		}
	}

	for _, b := range bs {
		if !req.Fill(block.NewSuccessOption(b)) {
			r.rq.PopBack()
		}
	}
}

func (r *Requester) onErrorResponse(msg Message) {
	id, reqErr, err := ErrorResponse(msg)
	if err != nil {
		return
	}

	req := r.rq.BackPopDone()
	if req == nil {
		return
	}

	if req.ID() != id {
		log.Warnf("Received Error response for wrong request(%d), skipping...", id)
		return
	}

	req.Error(reqErr)
}

func newBlockCheckCid(data []byte, expected cid.Cid) (blocks.Block, error) {
	actual, err := expected.Prefix().Sum(data)
	if err != nil {
		return nil, err
	}

	b, _ := blocks.NewBlockWithCid(data, actual)
	if !expected.Equals(actual) {
		return b, blocks.ErrWrongHash
	}

	return b, nil
}
