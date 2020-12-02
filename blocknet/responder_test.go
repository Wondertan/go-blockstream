package blocknet

import (
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/Wondertan/go-blockstream/block"
)

func TestResponder(t *testing.T) {
	rm := newFakeRequestManager()


	r, m := newTestResponder(rm)

	m.Send()
}

type fakePeerRequest struct {

}

type fakeRequestManager struct {
	reqs map[block.RequestID]block.Request
	l sync.Mutex
}

func newFakeRequestManager() RequestManager {
	return nil
}

func (f *fakeRequestManager) NewRequest(id block.RequestID, id2 peer.ID, cids []cid.Cid) block.Request {

}

func (f *fakeRequestManager) Request(id block.RequestID, id2 peer.ID) block.Request {

}
