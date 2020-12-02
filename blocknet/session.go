package blocknet

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"

	"github.com/Wondertan/go-blockstream/block"
)

type session struct {
	ctx context.Context

	reqs   map[peer.ID]*Requester
	reps   map[peer.ID]*Responder
	groups map[block.RequestID]*block.RequestGroup

	net Network
}

func NewSession(ctx context.Context, net Network, prvs []peer.ID) (*session, error) {
	ses := &session{
		ctx:    ctx,
		reqs:   make(map[peer.ID]*Requester, len(prvs)),
		reps:   make(map[peer.ID]*Responder),
		groups: make(map[block.RequestID]*block.RequestGroup),
	}

	for _, p := range prvs { // TODO Parallel
		r, err := net.Requester(ctx, p)
		if err != nil {
			return nil, err
		}

		ses.reqs[p] = r
	}

	return ses, nil
}

func (ses *session) Request(ctx context.Context, id block.RequestID, p peer.ID, ids ...cid.Cid) block.Request {
	rg, ok := ses.groups[id]
	if !ok {
		reqs := make([]block.Request, 0, len(ses.reqs))
		for _, rq := range ses.reqs {
			reqs = append(reqs, rq.NewRequest(id, ids))
		}

		rg = block.NewRequestGroup(ses.ctx, id)
		rg.Add(reqs...)

		ses.groups[id] = rg
	}

	if !rg.Init() {
		rg.SetIds(ids)
	}
	return rg.RequestFor(ctx, p)
}

func requestID(ids []cid.Cid) (block.RequestID, error) {
	p := ids[0].Prefix()
	dl := multihash.DefaultLengths[p.MhType]
	hashes := make([]byte, len(ids)*dl)
	for i, id := range ids {
		copy(hashes[i*dl:(i+1)*dl], id.Hash()[:dl])
	}

	return p.Sum(hashes)
}
